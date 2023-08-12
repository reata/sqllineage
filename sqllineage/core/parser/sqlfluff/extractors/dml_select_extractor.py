from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.parser.sqlfluff.extractors.lineage_holder_extractor import (
    LineageHolderExtractor,
)
from sqllineage.core.parser.sqlfluff.handlers.base import ConditionalSegmentBaseHandler
from sqllineage.core.parser.sqlfluff.models import SqlFluffTable
from sqllineage.core.parser.sqlfluff.utils import (
    get_child,
    get_children,
    list_child_segments,
)
from sqllineage.utils.entities import AnalyzerContext
from sqllineage.utils.helpers import escape_identifier_name


class DmlSelectExtractor(LineageHolderExtractor):
    """
    DML Select queries lineage extractor
    """

    SUPPORTED_STMT_TYPES = ["select_statement", "set_expression", "bracketed"]

    def __init__(self, dialect: str):
        super().__init__(dialect)

    def extract(
        self,
        statement: BaseSegment,
        context: AnalyzerContext,
    ) -> SubQueryLineageHolder:
        """
        Extract lineage for a given statement.
        :param statement: a sqlfluff segment with a statement
        :param context: 'AnalyzerContext'
        :return 'SubQueryLineageHolder' object
        """
        # to support SELECT INTO in some dialects, we also need to initialize target handler here
        handlers = [
            handler_cls()
            for handler_cls in ConditionalSegmentBaseHandler.__subclasses__()
        ]
        holder = self._init_holder(context)
        subqueries = []
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
            holder |= self.extract(
                sq.query, AnalyzerContext(cte=holder.cte, write={sq})
            )

        return holder

    @staticmethod
    def _handle_swap_partition(segment: BaseSegment, holder: SubQueryLineageHolder):
        """
        A handler for swap_partitions_between_tables function
        """
        if select_clause_element := get_child(segment, "select_clause_element"):
            if function := get_child(select_clause_element, "function"):
                if (
                    function.first_non_whitespace_segment_raw_upper
                    == "SWAP_PARTITIONS_BETWEEN_TABLES"
                ):
                    if bracketed := get_child(function, "bracketed"):
                        expressions = get_children(bracketed, "expression")
                        holder.add_read(
                            SqlFluffTable(escape_identifier_name(expressions[0].raw))
                        )
                        holder.add_write(
                            SqlFluffTable(escape_identifier_name(expressions[3].raw))
                        )
