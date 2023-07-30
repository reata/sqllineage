from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.models import AnalyzerContext
from sqllineage.core.parser.sqlfluff.extractors.lineage_holder_extractor import (
    LineageHolderExtractor,
)
from sqllineage.core.parser.sqlfluff.handlers.base import ConditionalSegmentBaseHandler
from sqllineage.core.parser.sqlfluff.models import SqlFluffSubQuery, SqlFluffTable
from sqllineage.core.parser.sqlfluff.utils import (
    get_grandchild,
    get_grandchildren,
    list_child_segments,
)
from sqllineage.utils.helpers import escape_identifier_name


class DmlSelectExtractor(LineageHolderExtractor):
    """
    DML Select queries lineage extractor
    """

    SUPPORTED_STMT_TYPES = ["select_statement", "set_expression"]

    def __init__(self, dialect: str):
        super().__init__(dialect)

    def extract(
        self,
        statement: BaseSegment,
        context: AnalyzerContext,
        is_sub_query: bool = False,
    ) -> SubQueryLineageHolder:
        """
        Extract lineage for a given statement.
        :param statement: a sqlfluff segment with a statement
        :param context: 'AnalyzerContext'
        :param is_sub_query: determine if the statement is bracketed or not
        :return 'SubQueryLineageHolder' object
        """
        # to support SELECT INTO in some dialects, we also need to initialize target handler here
        handlers = [
            handler_cls()
            for handler_cls in ConditionalSegmentBaseHandler.__subclasses__()
        ]
        holder = self._init_holder(context)
        subqueries = [SqlFluffSubQuery.of(statement, None)] if is_sub_query else []
        segments = (
            [statement]
            if statement.type == "set_expression"
            else list_child_segments(statement)
        )
        for segment in segments:
            for sq in self.parse_subquery(segment):
                # Collecting subquery on the way, hold on parsing until last
                # so that each handler don't have to worry about what's inside subquery
                subqueries.append(sq)

            self._handle_swap_partition(segment, holder)

            for handler in handlers:
                if handler.indicate(segment):
                    handler.handle(segment, holder)

        # call end of query hook here as loop is over
        for handler in handlers:
            handler.end_of_query_cleanup(holder)

        # By recursively extracting each subquery of the parent and merge, we're doing Depth-first search
        for sq in subqueries:
            holder |= self.extract(sq.query, AnalyzerContext(sq, holder.cte))

        for sq in holder.extra_subqueries:
            holder |= self.extract(sq.query, AnalyzerContext(sq, holder.cte))

        return holder

    @staticmethod
    def _handle_swap_partition(segment: BaseSegment, holder: SubQueryLineageHolder):
        """
        A handler for swap_partitions_between_tables function
        """
        function_from_select = get_grandchild(
            segment, "select_clause_element", "function"
        )
        if (
            function_from_select
            and function_from_select.first_non_whitespace_segment_raw_upper
            == "SWAP_PARTITIONS_BETWEEN_TABLES"
        ):
            function_parameters = get_grandchildren(
                function_from_select, "bracketed", "expression"
            )
            holder.add_read(
                SqlFluffTable(escape_identifier_name(function_parameters[0].raw))
            )
            holder.add_write(
                SqlFluffTable(escape_identifier_name(function_parameters[3].raw))
            )
