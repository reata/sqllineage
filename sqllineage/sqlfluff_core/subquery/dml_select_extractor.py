from sqlfluff.core.parser import BaseSegment

from sqllineage.sqlfluff_core.holders import SqlFluffSubQueryLineageHolder
from sqllineage.sqlfluff_core.models import SqlFluffAnalyzerContext, SqlFluffSubQuery
from sqllineage.sqlfluff_core.subquery.lineage_holder_extractor import (
    LineageHolderExtractor,
)
from sqllineage.sqlfluff_core.utils.sqlfluff import retrieve_segments


class DmlSelectExtractor(LineageHolderExtractor):
    """
    DML Select queries lineage extractor
    """

    DML_SELECT_STMT_TYPES = ["select_statement"]

    def __init__(self, dialect: str):
        super().__init__(dialect)

    def can_extract(self, statement_type: str) -> bool:
        """
        Determine if the current lineage holder extractor can process the statement
        :param statement_type: a sqlfluff segment type
        """
        return statement_type in self.DML_SELECT_STMT_TYPES

    def extract(
        self,
        statement: BaseSegment,
        context: SqlFluffAnalyzerContext,
        is_sub_query: bool = False,
    ) -> SqlFluffSubQueryLineageHolder:
        """
        Extract lineage for a given statement.
        :param statement: a sqlfluff segment with a statement
        :param context: 'SqlFluffAnalyzerContext'
        :param is_sub_query: determine if the statement is bracketed or not
        :return 'SqlFluffSubQueryLineageHolder' object
        """
        handlers, conditional_handlers = self._init_handlers()
        holder = self._init_holder(context)
        subqueries = [SqlFluffSubQuery.of(statement, None)] if is_sub_query else []
        segments = retrieve_segments(statement)
        for segment in segments:
            for sq in self.parse_subquery(segment):
                # Collecting subquery on the way, hold on parsing until last
                # so that each handler don't have to worry about what's inside subquery
                subqueries.append(sq)

            for current_handler in handlers:
                current_handler.handle(segment, holder)

            for conditional_handler in conditional_handlers:
                if conditional_handler.indicate(segment):
                    conditional_handler.handle(segment, holder)

        # call end of query hook here as loop is over
        for conditional_handler in conditional_handlers:
            conditional_handler.end_of_query_cleanup(holder)

        # By recursively extracting each subquery of the parent and merge, we're doing Depth-first search
        for sq in subqueries:
            holder |= self.extract(sq.segment, SqlFluffAnalyzerContext(sq, holder.cte))

        for sq in holder.extra_subqueries:
            holder |= self.extract(sq.segment, SqlFluffAnalyzerContext(sq, holder.cte))

        return holder
