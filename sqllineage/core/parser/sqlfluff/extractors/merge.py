from typing import Optional, Union

from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import StatementLineageHolder
from sqllineage.core.models import Column, SubQuery, Table
from sqllineage.core.parser.sqlfluff.extractors.base import BaseExtractor
from sqllineage.core.parser.sqlfluff.extractors.cte import CteExtractor
from sqllineage.core.parser.sqlfluff.extractors.select import SelectExtractor
from sqllineage.core.parser.sqlfluff.models import SqlFluffSubQuery
from sqllineage.core.parser.sqlfluff.utils import (
    extract_column_qualifier,
    extract_identifier,
    extract_innermost_bracketed,
    get_child,
    get_children,
    list_child_segments,
)
from sqllineage.utils.entities import AnalyzerContext


class MergeExtractor(BaseExtractor):
    """
    Merge statement lineage extractor
    """

    SUPPORTED_STMT_TYPES = ["merge_statement"]

    def extract(
        self,
        statement: BaseSegment,
        context: AnalyzerContext,
    ) -> StatementLineageHolder:
        holder = StatementLineageHolder()
        src_flag = tgt_flag = False
        direct_source: Optional[Union[Table, SubQuery]] = None
        segments = list_child_segments(statement)
        for i, segment in enumerate(segments):
            if segment.type == "merge_match":
                merge_match = segment
                for merge_when_matched_clause in get_children(
                    merge_match, "merge_when_matched_clause"
                ):
                    if merge_update_clause := get_child(
                        merge_when_matched_clause, "merge_update_clause"
                    ):
                        if set_clause_list := get_child(
                            merge_update_clause, "set_clause_list"
                        ):
                            for set_clause in get_children(
                                set_clause_list, "set_clause"
                            ):
                                columns = get_children(set_clause, "column_reference")
                                if len(columns) == 2:
                                    src_col = tgt_col = None
                                    if src_cqt := extract_column_qualifier(columns[1]):
                                        src_col = Column(src_cqt.column)
                                        src_col.parent = direct_source
                                    if tgt_cqt := extract_column_qualifier(columns[0]):
                                        tgt_col = Column(tgt_cqt.column)
                                        tgt_col.parent = list(holder.write)[0]
                                    if src_col is not None and tgt_col is not None:
                                        holder.add_column_lineage(src_col, tgt_col)
                for merge_when_not_matched_clause in get_children(
                    merge_match, "merge_when_not_matched_clause"
                ):
                    merge_insert = get_child(
                        merge_when_not_matched_clause, "merge_insert_clause"
                    )
                    insert_columns = []
                    if bracketed := get_child(merge_insert, "bracketed"):
                        for column_reference in get_children(
                            bracketed, "column_reference"
                        ):
                            if cqt := extract_column_qualifier(column_reference):
                                tgt_col = Column(cqt.column)
                                tgt_col.parent = list(holder.write)[0]
                                insert_columns.append(tgt_col)
                        if values_clause := get_child(merge_insert, "values_clause"):
                            if bracketed := get_child(values_clause, "bracketed"):
                                for j, e in enumerate(
                                    get_children(bracketed, "literal", "expression")
                                ):
                                    if column_reference := get_child(
                                        e, "column_reference"
                                    ):
                                        if cqt := extract_column_qualifier(
                                            column_reference
                                        ):
                                            src_col = Column(cqt.column)
                                            src_col.parent = direct_source
                                            holder.add_column_lineage(
                                                src_col, insert_columns[j]
                                            )
            elif segment.type == "keyword":
                if segment.raw_upper in ["MERGE", "INTO"]:
                    tgt_flag = True
                elif segment.raw_upper == "USING":
                    src_flag = True
                continue

            if tgt_flag:
                if table := self.find_table(segment):
                    holder.add_write(table)
                tgt_flag = False
            if src_flag:
                if table := self.find_table(segment):
                    holder.add_read(table)
                    direct_source = table
                elif segment.type == "bracketed":
                    next_segment = segments[i + 1]
                    direct_source = SqlFluffSubQuery.of(
                        extract_innermost_bracketed(segment),
                        extract_identifier(next_segment)
                        if next_segment.type == "alias_expression"
                        else None,
                    )
                    holder.add_read(direct_source)

                    extractor_cls = (
                        CteExtractor
                        if direct_source.query.get_child(
                            "with_compound_statement"
                        )  # in case the subquery is a CTE query
                        else SelectExtractor  # in case the subquery is a select query
                    )
                    holder |= self.delegate_to(
                        extractor_cls,
                        direct_source.query,
                        AnalyzerContext(cte=holder.cte, write={direct_source}),
                    )
                src_flag = False
        return holder
