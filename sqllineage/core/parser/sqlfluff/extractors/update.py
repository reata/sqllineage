from typing import Optional, Union

from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.metadata_provider import MetaDataProvider
from sqllineage.core.models import Column, Path, SubQuery, Table
from sqllineage.core.parser import SourceHandlerMixin
from sqllineage.core.parser.sqlfluff.extractors.base import BaseExtractor
from sqllineage.core.parser.sqlfluff.models import SqlFluffSubQuery, SqlFluffTable
from sqllineage.core.parser.sqlfluff.utils import (
    extract_column_qualifier,
    find_from_expression_element,
    find_table_identifier,
    list_child_segments,
    list_join_clause,
    list_subqueries,
)
from sqllineage.utils.entities import AnalyzerContext
from sqllineage.utils.helpers import escape_identifier_name


class UpdateExtractor(BaseExtractor, SourceHandlerMixin):
    """
    Update statement lineage extractor
    """

    def __init__(self, dialect: str, metadata_provider: MetaDataProvider):
        super().__init__(dialect, metadata_provider)
        self.columns = []
        self.tables = []
        self.union_barriers = []

    SUPPORTED_STMT_TYPES = ["update_statement"]

    def extract(
        self, statement: BaseSegment, context: AnalyzerContext
    ) -> SubQueryLineageHolder:
        holder = self._init_holder(context)
        tgt_flag = False
        subqueries = []
        for segment in list_child_segments(statement):
            for sq in self.list_subquery(segment):
                # Collecting subquery on the way, hold on parsing until last
                # so that each handler don't have to worry about what's inside subquery
                subqueries.append(sq)

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
                    columns = set_clause.get_children("column_reference")
                    if len(columns) == 2:
                        tgt_cqt = extract_column_qualifier(columns[0])
                        src_cqt = extract_column_qualifier(columns[1])
                        if tgt_cqt is not None and src_cqt is not None:
                            self.columns.append(
                                Column(tgt_cqt.column, source_columns=[src_cqt])
                            )

            if segment.type == "from_clause":
                # UPDATE FROM, ansi syntax
                self._handle_table(segment, holder)

        self.end_of_query_cleanup(holder)

        self.extract_subquery(subqueries, holder)

        return holder

    def _handle_table(
        self, segment: BaseSegment, holder: SubQueryLineageHolder
    ) -> None:
        """
        handle from_clause or join_clause, join_clause is a child node of from_clause.
        """
        if segment.type in ["from_clause", "join_clause"]:
            from_expressions = segment.get_children("from_expression")
            if len(from_expressions) > 1:
                # SQL89 style of join
                for from_expression in from_expressions:
                    if from_expression_element := find_from_expression_element(
                        from_expression
                    ):
                        self._add_dataset_from_expression_element(
                            from_expression_element, holder
                        )
            else:
                if from_expression_element := find_from_expression_element(segment):
                    self._add_dataset_from_expression_element(
                        from_expression_element, holder
                    )
                for join_clause in list_join_clause(segment):
                    self._handle_table(join_clause, holder)

    def _add_dataset_from_expression_element(
        self, segment: BaseSegment, holder: SubQueryLineageHolder
    ) -> None:
        """
        Append tables and subqueries identified in the 'from_expression_element' type segment to the table and
        holder extra subqueries sets
        """
        all_segments = [
            seg for seg in list_child_segments(segment) if seg.type != "keyword"
        ]
        if table_expression := segment.get_child("table_expression"):
            if table_expression.get_child("function"):
                # for UNNEST or generator function, no dataset involved
                return
        first_segment = all_segments[0]
        if first_segment.type == "bracketed":
            if table_expression := first_segment.get_child("table_expression"):
                if table_expression.get_child("values_clause"):
                    # (VALUES ...) AS alias, no dataset involved
                    return
        subqueries = list_subqueries(segment)
        if subqueries:
            for sq in subqueries:
                bracketed, alias = sq
                read_sq = SqlFluffSubQuery.of(bracketed, alias)
                self.tables.append(read_sq)
        else:
            table_identifier = find_table_identifier(segment)
            if table_identifier:
                subquery_flag = False
                alias = None
                if len(all_segments) > 1 and all_segments[1].type == "alias_expression":
                    all_segments = list_child_segments(all_segments[1])
                    alias = str(
                        all_segments[1].raw
                        if len(all_segments) > 1
                        else all_segments[0].raw
                    )
                if "." not in table_identifier.raw:
                    cte_dict = {s.alias: s for s in holder.cte}
                    cte = cte_dict.get(table_identifier.raw)
                    if cte is not None:
                        # could reference CTE with or without alias
                        self.tables.append(
                            SqlFluffSubQuery.of(
                                cte.query,
                                alias or table_identifier.raw,
                            )
                        )
                        subquery_flag = True
                if subquery_flag is False:
                    if table_identifier.type == "file_reference":
                        self.tables.append(
                            Path(
                                escape_identifier_name(
                                    table_identifier.segments[-1].raw
                                )
                            )
                        )
                    else:
                        self.tables.append(
                            SqlFluffTable.of(table_identifier, alias=alias)
                        )

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
