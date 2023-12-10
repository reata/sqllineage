from typing import Optional, Union

from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.models import SubQuery, Table
from sqllineage.core.parser.sqlfluff.extractors.base import BaseExtractor
from sqllineage.core.parser.sqlfluff.models import SqlFluffSubQuery
from sqllineage.core.parser.sqlfluff.utils import (
    find_from_expression_element,
    find_table_identifier,
    list_child_segments,
    list_join_clause,
)
from sqllineage.utils.entities import AnalyzerContext


class UpdateExtractor(BaseExtractor):
    """
    Update statement lineage extractor
    """

    SUPPORTED_STMT_TYPES = ["update_statement"]

    def extract(
        self, statement: BaseSegment, context: AnalyzerContext
    ) -> SubQueryLineageHolder:
        holder = self._init_holder(context)
        tgt_flag = False
        for segment in list_child_segments(statement):
            if segment.type == "from_expression":
                # UPDATE with JOIN, mysql only syntax
                if table := self.find_table_from_from_expression_or_join_clause(
                    segment, holder
                ):
                    holder.add_write(table)
                for join_clause in list_join_clause(statement):
                    if table := self.find_table_from_from_expression_or_join_clause(
                        join_clause, holder
                    ):
                        holder.add_read(table)

            if segment.type == "keyword" and segment.raw_upper == "UPDATE":
                tgt_flag = True
                continue

            if tgt_flag:
                if table := self.find_table(segment):
                    holder.add_write(table)
                tgt_flag = False

            if segment.type == "from_clause":
                # UPDATE FROM, ansi syntax
                if from_expression := segment.get_child("from_expression"):
                    if table := self.find_table_from_from_expression_or_join_clause(
                        from_expression, holder
                    ):
                        holder.add_read(table)

        return holder

    def find_table_from_from_expression_or_join_clause(
        self, segment, holder
    ) -> Optional[Union[Table, SubQuery]]:
        table: Optional[Union[Table, SubQuery]] = None
        if from_expression_element := find_from_expression_element(segment):
            if table_identifier := find_table_identifier(from_expression_element):
                cte_dict = {c.alias: c for c in holder.cte}
                if cte := cte_dict.get(table_identifier.raw):
                    table = SqlFluffSubQuery.of(cte.query, table_identifier.raw)
                else:
                    table = self.find_table(table_identifier)
        return table
