from typing import Optional, Union

from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import StatementLineageHolder, SubQueryLineageHolder
from sqllineage.core.models import Column, SubQuery, Table
from sqllineage.core.parser.sqlfluff.extractors.cte_extractor import DmlCteExtractor
from sqllineage.core.parser.sqlfluff.extractors.dml_select_extractor import (
    DmlSelectExtractor,
)
from sqllineage.core.parser.sqlfluff.extractors.lineage_holder_extractor import (
    LineageHolderExtractor,
)
from sqllineage.core.parser.sqlfluff.models import SqlFluffSubQuery, SqlFluffTable
from sqllineage.core.parser.sqlfluff.utils import (
    extract_identifier,
    extract_innermost_bracketed,
    get_child,
    get_children,
    list_child_segments,
)
from sqllineage.utils.entities import AnalyzerContext


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
    ) -> SubQueryLineageHolder:
        holder = StatementLineageHolder()
        src_flag = tgt_flag = False
        direct_source: Optional[Union[Table, SubQuery]] = None
        segments = list_child_segments(statement)
        for i, segment in enumerate(segments):
            if segment.type == "keyword" and segment.raw_upper in {"INTO", "MERGE"}:
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
                        extract_innermost_bracketed(segment),
                        extract_identifier(next_segment)
                        if next_segment.type == "alias_expression"
                        else None,
                    )
                    holder.add_read(direct_source)
                    if direct_source.query.get_child("with_compound_statement"):
                        # in case the subquery is a CTE query
                        holder |= DmlCteExtractor(self.dialect).extract(
                            direct_source.query,
                            AnalyzerContext(cte=holder.cte, write={direct_source}),
                        )
                    else:
                        # in case the subquery is a select query
                        holder |= DmlSelectExtractor(self.dialect).extract(
                            direct_source.query,
                            AnalyzerContext(cte=holder.cte, write={direct_source}),
                        )
                src_flag = False

        if merge_match := get_child(statement, "merge_match"):
            for merge_when_matched_clause in get_children(
                merge_match, "merge_when_matched_clause"
            ):
                if merge_update_clause := get_child(
                    merge_when_matched_clause, "merge_update_clause"
                ):
                    if set_clause_list := get_child(
                        merge_update_clause, "set_clause_list"
                    ):
                        for set_clause in get_children(set_clause_list, "set_clause"):
                            columns = get_children(set_clause, "column_reference")
                            if len(columns) == 2:
                                src_col = Column(extract_identifier(columns[1]))
                                src_col.parent = direct_source
                                tgt_col = Column(extract_identifier(columns[0]))
                                tgt_col.parent = list(holder.write)[0]
                                holder.add_column_lineage(src_col, tgt_col)
            for merge_when_not_matched_clause in get_children(
                merge_match, "merge_when_not_matched_clause"
            ):
                merge_insert = get_child(
                    merge_when_not_matched_clause, "merge_insert_clause"
                )
                insert_columns = []
                if bracketed := get_child(merge_insert, "bracketed"):
                    for column_reference in get_children(bracketed, "column_reference"):
                        tgt_col = Column(extract_identifier(column_reference))
                        tgt_col.parent = list(holder.write)[0]
                        insert_columns.append(tgt_col)
                    if values_clause := get_child(merge_insert, "values_clause"):
                        if bracketed := get_child(values_clause, "bracketed"):
                            for j, e in enumerate(
                                get_children(bracketed, "literal", "expression")
                            ):
                                if column_reference := get_child(e, "column_reference"):
                                    src_col = Column(
                                        extract_identifier(column_reference)
                                    )
                                    src_col.parent = direct_source
                                    holder.add_column_lineage(
                                        src_col, insert_columns[j]
                                    )
        return holder
