import re
from typing import Dict, List, Tuple, Union

from sqlfluff.core.parser import BaseSegment

from sqllineage.exceptions import SQLLineageException
from sqllineage.sqlfluff_core.handlers.base import ConditionalSegmentBaseHandler
from sqllineage.sqlfluff_core.holders import SqlFluffSubQueryLineageHolder
from sqllineage.sqlfluff_core.models import (
    SqlFluffColumn,
    SqlFluffSubQuery,
)
from sqllineage.sqlfluff_core.models import (
    SqlFluffPath,
    SqlFluffTable,
)
from sqllineage.sqlfluff_core.utils.holder import retrieve_holder_data_from
from sqllineage.sqlfluff_core.utils.sqlfluff import (
    find_table_identifier,
    get_grandchild,
    get_inner_from_expression,
    get_multiple_identifiers,
    get_subqueries,
    is_subquery,
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
        self.columns: List[SqlFluffColumn] = []
        self.tables: List[
            Union[SqlFluffPath, SqlFluffTable, SqlFluffSubQuery, SqlFluffSubQuery]
        ] = []
        self.union_barriers: List[Tuple[int, int]] = []

    def indicate(self, segment: BaseSegment) -> bool:
        """
        Indicates if the handler can handle the segment
        :param segment: segment to be processed
        :return: True if it can be handled
        """
        return self._indicate_column(segment) or self._indicate_table(segment)

    def handle(
        self, segment: BaseSegment, holder: SqlFluffSubQueryLineageHolder
    ) -> None:
        """
        Handle the segment, and update the lineage result accordingly in the holder
        :param segment: segment to be handled
        :param holder: 'SqlFluffSubQueryLineageHolder' to hold lineage
        """
        if self._indicate_table(segment):
            self._handle_table(segment, holder)
        if self._indicate_column(segment):
            self._handle_column(segment)

    def _handle_table(
        self, segment: BaseSegment, holder: SqlFluffSubQueryLineageHolder
    ) -> None:
        """
        Table handler method
        :param segment: segment to be handled
        :param holder: 'SqlFluffSubQueryLineageHolder' to hold lineage
        """
        identifiers = get_multiple_identifiers(segment)
        if identifiers and len(identifiers) > 1:
            for identifier in identifiers:
                self._add_dataset_from_expression_element(identifier, holder)
        from_segment = get_inner_from_expression(segment)
        if from_segment.type == "from_expression_element":
            self._add_dataset_from_expression_element(from_segment, holder)
        elif from_segment.type == "bracketed":
            if is_subquery(from_segment):
                self.tables.append(SqlFluffSubQuery.of(from_segment, None))
        else:
            raise SQLLineageException(
                "An 'from_expression_element' or 'bracketed' segment is expected, got %s instead."
                % from_segment.type
            )
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

    def end_of_query_cleanup(self, holder: SqlFluffSubQueryLineageHolder) -> None:
        """
        Optional method to be called at the end of statement or subquery
        :param holder: 'SqlFluffSubQueryLineageHolder' to hold lineage
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
        self, segment: BaseSegment, holder: SqlFluffSubQueryLineageHolder
    ) -> None:
        """
        Append tables and subqueries identified in the 'from_expression_element' type segment to the table and
        holder extra subqueries sets
        :param segment: 'from_expression_element' type segment
        :param holder: 'SqlFluffSubQueryLineageHolder' to hold lineage
        """
        dataset: Union[SqlFluffPath, SqlFluffTable, SqlFluffSubQuery]
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
        path_match = re.match(r"(parquet|csv|json)\.`(.*)`", segment.raw_upper)
        if path_match is not None:
            dataset = SqlFluffPath(path_match.groups()[1])
        else:
            subqueries = get_subqueries(segment, skip_union=False)
            if subqueries:
                for sq in subqueries:
                    bracketed, alias = sq
                    read_sq = SqlFluffSubQuery.of(bracketed, alias)
                    holder.extra_subqueries.add(read_sq)
                    self.tables.append(read_sq)
                return
            else:
                table_identifier = find_table_identifier(segment)
                if table_identifier:
                    dataset = retrieve_holder_data_from(
                        all_segments, holder, table_identifier
                    )
                else:
                    return
        self.tables.append(dataset)

    def _get_alias_mapping_from_table_group(
        self,
        table_group: List[
            Union[SqlFluffPath, SqlFluffTable, SqlFluffSubQuery, SqlFluffSubQuery]
        ],
        holder: SqlFluffSubQueryLineageHolder,
    ) -> Dict[str, Union[SqlFluffTable, SqlFluffSubQuery]]:
        """
        A table can be referred to as alias, table name, or database_name.table_name, create the mapping here.
        For SubQuery, it's only alias then.
        :param table_group: a list of objects from the table list
        :param holder: 'SqlFluffSubQueryLineageHolder' to hold lineage
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
