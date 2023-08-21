from typing import Optional

from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import StatementLineageHolder
from sqllineage.core.models import Table
from sqllineage.core.parser.sqlfluff.extractors.base import BaseExtractor
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
    ) -> StatementLineageHolder:
        holder = StatementLineageHolder()
        tgt_flag = False
        for segment in list_child_segments(statement):
            if segment.type == "from_expression":
                # UPDATE with JOIN
                if table := self.find_table_from_from_expression_or_join_clause(
                    segment
                ):
                    holder.add_write(table)
                for join_clause in list_join_clause(statement):
                    if table := self.find_table_from_from_expression_or_join_clause(
                        join_clause
                    ):
                        holder.add_read(table)

            if segment.type == "keyword" and segment.raw_upper == "UPDATE":
                tgt_flag = True
                continue

            if tgt_flag:
                if table := self.find_table(segment):
                    holder.add_write(table)
                tgt_flag = False

        return holder

    def find_table_from_from_expression_or_join_clause(
        self, segment
    ) -> Optional[Table]:
        table = None
        if from_expression_element := find_from_expression_element(segment):
            if table_identifier := find_table_identifier(from_expression_element):
                table = self.find_table(table_identifier)
        return table
