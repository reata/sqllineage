from typing import Optional, Union

from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import StatementLineageHolder, SubQueryLineageHolder
from sqllineage.core.models import AnalyzerContext, Column, SubQuery, Table
from sqllineage.core.parser.sqlfluff.extractors.dml_select_extractor import (
    DmlSelectExtractor,
)
from sqllineage.core.parser.sqlfluff.extractors.lineage_holder_extractor import (
    LineageHolderExtractor,
)
from sqllineage.core.parser.sqlfluff.models import SqlFluffSubQuery, SqlFluffTable
from sqllineage.core.parser.sqlfluff.utils import (
    get_grandchildren,
    get_identifier,
    get_innermost_bracketed,
    retrieve_segments,
)


class DmlMergeExtractor(LineageHolderExtractor):
    """
    DDL Alter queries lineage extractor
    """

    SUPPORTED_STMT_TYPES = ["merge_statement"]

    def __init__(self, dialect: str):
        super().__init__(dialect)

    def extract(
        self,
        statement: BaseSegment,
        context: AnalyzerContext,
        is_sub_query: bool = False,
    ) -> SubQueryLineageHolder:
        holder = StatementLineageHolder()
        src_flag = tgt_flag = False
        direct_source: Optional[Union[Table, SubQuery]] = None
        segments = retrieve_segments(statement)
        for i, segment in enumerate(segments):
            if segment.type == "keyword" and segment.raw_upper == "INTO":
                tgt_flag = True
                continue
            if segment.type == "keyword" and segment.raw_upper == "USING":
                src_flag = True
                continue

            if tgt_flag:
                if segment.type == "table_reference":
                    holder.add_write(SqlFluffTable.of(segment))
                tgt_flag = False
            if src_flag:
                if segment.type == "table_reference":
                    direct_source = SqlFluffTable.of(segment)
                    holder.add_read(direct_source)
                elif segment.type == "bracketed":
                    next_segment = segments[i + 1]
                    direct_source = SqlFluffSubQuery.of(
                        get_innermost_bracketed(segment),
                        get_identifier(next_segment)
                        if next_segment.type == "alias_expression"
                        else None,
                    )
                    holder.add_read(direct_source)
                    holder |= DmlSelectExtractor(self.dialect).extract(
                        direct_source.query, AnalyzerContext(direct_source, holder.cte)
                    )
                src_flag = False

        for match in get_grandchildren(
            statement, "merge_match", "merge_when_matched_clause"
        ):
            if (
                merge_update_clause := match.get_child("merge_update_clause")
            ) is not None:
                for set_clause in get_grandchildren(
                    merge_update_clause, "set_clause_list", "set_clause"
                ):
                    columns = set_clause.get_children("column_reference")
                    if len(columns) == 2:
                        src_col = Column(get_identifier(columns[1]))
                        src_col.parent = direct_source
                        tgt_col = Column(get_identifier(columns[0]))
                        tgt_col.parent = list(holder.write)[0]
                        holder.add_column_lineage(src_col, tgt_col)
        for not_match in get_grandchildren(
            statement, "merge_match", "merge_when_not_matched_clause"
        ):
            merge_insert = not_match.get_child("merge_insert_clause")
            insert_columns = []
            for c in merge_insert.get_child("bracketed").get_children(
                "column_reference"
            ):
                tgt_col = Column(get_identifier(c))
                tgt_col.parent = list(holder.write)[0]
                insert_columns.append(tgt_col)
            for j, e in enumerate(
                merge_insert.get_child("values_clause")
                .get_child("bracketed")
                .get_children("expression")
            ):
                if col_ref := e.get_child("column_reference"):
                    src_col = Column(get_identifier(col_ref))
                    src_col.parent = direct_source
                    holder.add_column_lineage(src_col, insert_columns[j])
        return holder
