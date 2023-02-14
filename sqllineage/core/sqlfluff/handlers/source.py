from typing import Dict, List, Tuple, Union

from sqlfluff.core.parser import BaseSegment

from sqllineage.core.exceptions import SQLLineageException
from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.models import Column, Path, SubQuery, Table
from sqllineage.core.sqlfluff.handlers.base import ConditionalSegmentBaseHandler
from sqllineage.core.sqlfluff.models import (
    SqlFluffColumn,
    SqlFluffSubQuery,
)
from sqllineage.core.sqlfluff.models import (
    SqlFluffTable,
)
from sqllineage.core.sqlfluff.utils.holder import retrieve_holder_data_from
from sqllineage.core.sqlfluff.utils.sqlfluff import (
    find_table_identifier,
    get_grandchild,
    get_inner_from_expression,
    get_multiple_identifiers,
    get_subqueries,
    is_values_clause,
    retrieve_extra_segment,
    retrieve_segments,
)
from sqllineage.utils.constant import EdgeType


class SourceHandler(ConditionalSegmentBaseHandler):
    """
    Source table and column handler
    """

    def __init__(self, dialect: str):
        super().__init__(dialect)
        self.columns: List[Column] = []
        self.tables: List[Union[Path, Table, SubQuery]] = []
        self.union_barriers: List[Tuple[int, int]] = []

    def indicate(self, segment: BaseSegment) -> bool:
        """
        Indicates if the handler can handle the segment
        :param segment: segment to be processed
        :return: True if it can be handled
        """
        return self._indicate_column(segment) or self._indicate_table(segment)

    def handle(self, segment: BaseSegment, holder: SubQueryLineageHolder) -> None:
        """
        Handle the segment, and update the lineage result accordingly in the holder
        :param segment: segment to be handled
        :param holder: 'SubQueryLineageHolder' to hold lineage
        """
        if self._indicate_table(segment):
            self._handle_table(segment, holder)
        if self._indicate_column(segment):
            self._handle_column(segment)

    def _handle_table(
        self, segment: BaseSegment, holder: SubQueryLineageHolder
    ) -> None:
        """
        Table handler method
        :param segment: segment to be handled
        :param holder: 'SubQueryLineageHolder' to hold lineage
        """
        identifiers = get_multiple_identifiers(segment)
        if identifiers and len(identifiers) > 1:
            for identifier in identifiers:
                self._add_dataset_from_expression_element(identifier, holder)
        from_segment = get_inner_from_expression(segment)
        if from_segment.type == "from_expression_element":
            self._add_dataset_from_expression_element(from_segment, holder)
        for extra_segment in retrieve_extra_segment(segment):
            self._handle_table(extra_segment, holder)

    def _handle_column(self, segment: BaseSegment) -> None:
        """
        Column handler method
        :param segment: segment to be handled
        """
        sub_segments = retrieve_segments(segment)
        for sub_segment in sub_segments:
            if sub_segment.type == "select_clause_element":
                self.columns.append(
                    SqlFluffColumn.of(sub_segment, dialect=self.dialect)
                )

    def end_of_query_cleanup(self, holder: SubQueryLineageHolder) -> None:
        """
        Optional method to be called at the end of statement or subquery
        :param holder: 'SubQueryLineageHolder' to hold lineage
        """
        for i, tbl in enumerate(self.tables):
            holder.add_read(tbl)
        self.union_barriers.append((len(self.columns), len(self.tables)))
        for i, (col_barrier, tbl_barrier) in enumerate(self.union_barriers):
            prev_col_barrier, prev_tbl_barrier = (
                (0, 0) if i == 0 else self.union_barriers[i - 1]
            )
            col_grp = self.columns[prev_col_barrier:col_barrier]
            tbl_grp = self.tables[prev_tbl_barrier:tbl_barrier]
            tgt_tbl = None
            if holder.write:
                if len(holder.write) > 1:
                    raise SQLLineageException
                tgt_tbl = list(holder.write)[0]
            if tgt_tbl:
                for tgt_col in col_grp:
                    tgt_col.parent = tgt_tbl
                    for src_col in tgt_col.to_source_columns(
                        self._get_alias_mapping_from_table_group(tbl_grp, holder)
                    ):
                        holder.add_column_lineage(src_col, tgt_col)

    def _add_dataset_from_expression_element(
        self, segment: BaseSegment, holder: SubQueryLineageHolder
    ) -> None:
        """
        Append tables and subqueries identified in the 'from_expression_element' type segment to the table and
        holder extra subqueries sets
        :param segment: 'from_expression_element' type segment
        :param holder: 'SubQueryLineageHolder' to hold lineage
        """
        dataset: Union[Path, Table, SubQuery]
        all_segments = [
            seg for seg in retrieve_segments(segment) if seg.type != "keyword"
        ]
        first_segment = all_segments[0]
        function_as_table = get_grandchild(segment, "table_expression", "function")
        if first_segment.type == "function" or function_as_table:
            # function() as alias, no dataset involved
            return
        elif first_segment.type == "bracketed" and is_values_clause(first_segment):
            # (VALUES ...) AS alias, no dataset involved
            return
        else:
            subqueries = get_subqueries(segment)
            if subqueries:
                for sq in subqueries:
                    bracketed, alias = sq
                    read_sq = SqlFluffSubQuery.of(bracketed, alias)
                    holder.extra_subqueries.add(read_sq)
                    self.tables.append(read_sq)
            else:
                table_identifier = find_table_identifier(segment)
                if table_identifier:
                    dataset = retrieve_holder_data_from(
                        all_segments, holder, table_identifier
                    )
                    self.tables.append(dataset)

    @staticmethod
    def _get_alias_mapping_from_table_group(
        table_group: List[Union[Path, Table, SubQuery]],
        holder: SubQueryLineageHolder,
    ) -> Dict[str, Union[Table, SubQuery]]:
        """
        A table can be referred to as alias, table name, or database_name.table_name, create the mapping here.
        For SubQuery, it's only alias then.
        :param table_group: a list of objects from the table list
        :param holder: 'SubQueryLineageHolder' to hold lineage
        :return: A map of tables and references
        """
        return {
            **{
                tgt: src
                for src, tgt, attr in holder.graph.edges(data=True)
                if attr.get("type") == EdgeType.HAS_ALIAS and src in table_group
            },
            **{
                table.raw_name: table
                for table in table_group
                if isinstance(table, SqlFluffTable)
            },
            **{
                str(table): table
                for table in table_group
                if isinstance(table, SqlFluffTable)
            },
        }

    @staticmethod
    def _indicate_column(segment: BaseSegment) -> bool:
        """
        Check if it is a column
        :param segment: segment to be checked
        :return: True if type is 'select_clause'
        """
        return bool(segment.type == "select_clause")

    @staticmethod
    def _indicate_table(segment: BaseSegment) -> bool:
        """
        Check if it is a table
        :param segment: segment to be checked
        :return: True if type is 'from_clause'
        """
        return bool(segment.type == "from_clause")
