from typing import Union

from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.models import Path, SubQuery, Table
from sqllineage.core.parser import SourceHandlerMixin
from sqllineage.core.parser.sqlfluff.handlers.base import ConditionalSegmentBaseHandler
from sqllineage.core.parser.sqlfluff.holder_utils import retrieve_holder_data_from
from sqllineage.core.parser.sqlfluff.models import (
    SqlFluffColumn,
    SqlFluffSubQuery,
    SqlFluffTable,
)
from sqllineage.core.parser.sqlfluff.utils import (
    find_table_identifier,
    get_grandchild,
    get_inner_from_expression,
    get_multiple_identifiers,
    get_subqueries,
    is_union,
    is_values_clause,
    retrieve_extra_segment,
    retrieve_segments,
)


class SourceHandler(SourceHandlerMixin, ConditionalSegmentBaseHandler):
    """
    Source table and column handler
    """

    def __init__(self):
        self.columns = []
        self.tables = []
        self.union_barriers = []

    def indicate(self, segment: BaseSegment) -> bool:
        """
        Indicates if the handler can handle the segment
        :param segment: segment to be processed
        :return: True if it can be handled
        """
        return (
            self._indicate_column(segment)
            or self._indicate_table(segment)
            or is_union(segment)
        )

    def handle(self, segment: BaseSegment, holder: SubQueryLineageHolder) -> None:
        """
        Handle the segment, and update the lineage result accordingly in the holder
        :param segment: segment to be handled
        :param holder: 'SubQueryLineageHolder' to hold lineage
        """
        if self._indicate_table(segment):
            self._handle_table(segment, holder)
        elif is_union(segment):
            self._handle_union(segment)
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
                self.columns.append(SqlFluffColumn.of(sub_segment))

    def _handle_union(self, segment: BaseSegment) -> None:
        """
        Union handler method
        :param segment: segment to be handled
        """
        subqueries = get_subqueries(segment)
        if subqueries:
            for idx, sq in enumerate(subqueries):
                if idx != 0:
                    self.union_barriers.append((len(self.columns), len(self.tables)))
                subquery, alias = sq
                table_identifier = find_table_identifier(subquery)
                if table_identifier:
                    read_sq = SqlFluffTable.of(table_identifier, alias)
                    segments = retrieve_segments(subquery)
                    for seg in segments:
                        if seg.type == "select_clause":
                            self._handle_column(seg)
                    self.tables.append(read_sq)

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
        elif is_union(segment):
            subqueries = get_subqueries(segment)
            subquery, alias = subqueries[0]
            self.tables.append(SqlFluffSubQuery.of(subquery, alias))
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
