from sqlfluff.core.parser import BaseSegment
from sqllineage.core.holders import SubQueryLineageHolder

from sqllineage.sqlfluff_core.models import SqlFluffAnalyzerContext
from sqllineage.sqlfluff_core.models import SqlFluffSubQuery

from sqllineage.sqlfluff_core.subquery.lineage_holder_extractor import (
    LineageHolderExtractor,
)
from sqllineage.sqlfluff_core.utils.sqlfluff import retrieve_segments


class DmlSelectExtractor(LineageHolderExtractor):

    DML_SELECT_STMT_TYPES = ["select_statement"]

    def can_extract(self, statement_type: str):
        return statement_type in self.DML_SELECT_STMT_TYPES

    def extract(
        self,
        statement: BaseSegment,
        context: SqlFluffAnalyzerContext,
        is_sub_query: bool = False,
    ) -> SubQueryLineageHolder:
        handlers, conditional_handlers = self._init_handlers()
        holder = self._init_holder(context)
        sub_queries = [SqlFluffSubQuery.of(statement, None)] if is_sub_query else []
        segments = retrieve_segments(statement)
        for segment in segments:
            for sq in self.parse_subquery(segment):
                # Collecting subquery on the way, hold on parsing until last
                # so that each handler don't have to worry about what's inside subquery
                sub_queries.append(sq)

            for current_handler in handlers:
                current_handler.handle(segment, holder)

            for conditional_handler in conditional_handlers:
                if conditional_handler.indicate(segment):
                    conditional_handler.handle(segment, holder)

        # call end of query hook here as loop is over
        for conditional_handler in conditional_handlers:
            conditional_handler.end_of_query_cleanup(holder)

        # By recursively extracting each subquery of the parent and merge, we're doing Depth-first search
        for sq in sub_queries:
            holder |= self.extract(sq.segment, SqlFluffAnalyzerContext(sq, holder.cte))

        return holder
