from typing import Optional, Union

from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.models import Column, SubQuery, Table
from sqllineage.core.parser import SourceHandlerMixin
from sqllineage.core.parser.sqlfluff.extractors.base import BaseExtractor
from sqllineage.core.parser.sqlfluff.models import SqlFluffSubQuery
from sqllineage.core.parser.sqlfluff.utils import (
    extract_column_qualifier,
    find_from_expression_element,
    find_table_identifier,
    list_child_segments,
    list_join_clause,
)
from sqllineage.utils.entities import AnalyzerContext


class UpdateExtractor(BaseExtractor, SourceHandlerMixin):
    """
    Update statement lineage extractor
    """

    SUPPORTED_STMT_TYPES = ["update_statement"]

    def extract(
        self, statement: BaseSegment, context: AnalyzerContext
    ) -> SubQueryLineageHolder:
        holder = self._init_holder(context)
        tgt_flag = False
        tables = []
        columns = []
        subqueries = []
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

            if segment.type == "set_clause_list":
                for set_clause in segment.get_children("set_clause"):
                    column_references = set_clause.get_children("column_reference")
                    if len(column_references) == 2:
                        tgt_cqt = extract_column_qualifier(column_references[0])
                        src_cqt = extract_column_qualifier(column_references[1])
                        if tgt_cqt is not None and src_cqt is not None:
                            columns.append(
                                Column(tgt_cqt.column, source_columns=[src_cqt])
                            )

            if segment.type == "from_clause":
                # UPDATE FROM, ansi syntax
                for sq in self.list_subquery(segment):
                    # Collecting subquery on the way, hold on parsing until last
                    # so that each handler don't have to worry about what's inside subquery
                    subqueries.append(sq)

                tables += self._list_table_from_from_clause_or_join_clause(
                    segment, holder
                )

        for table in tables:  # type: ignore
            holder.add_read(table)

        for tgt_col in columns:
            tgt_col.parent = list(holder.write)[0]
            for src_col in tgt_col.to_source_columns(
                self.get_alias_mapping_from_table_group(tables, holder)
            ):
                holder.add_column_lineage(src_col, tgt_col)

        self.extract_subquery(subqueries, holder)

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
