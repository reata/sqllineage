from sqlfluff.core.parser import BaseSegment
from sqllineage.sqlfluff_core.subquery.dml_select_extractor import DmlSelectExtractor

from sqllineage.core.holders import SubQueryLineageHolder

from sqllineage.sqlfluff_core.models import SqlFluffAnalyzerContext

from sqllineage.sqlfluff_core.subquery.lineage_holder_extractor import (
    LineageHolderExtractor,
)
from sqllineage.sqlfluff_core.utils.sqlfluff import retrieve_segments


class DmlInsertExtractor(LineageHolderExtractor):

    DML_INSERT_STMT_TYPES = [
        "insert_statement",
        "create_table_statement",
        "update_statement",
        "copy_statement",
        "insert_overwrite_directory_hive_fmt_statement",
        "copy_into_statement",
        "copy_into_table_statement",
    ]

    def can_extract(self, statement_type: str):
        return statement_type in self.DML_INSERT_STMT_TYPES

    def extract(
        self,
        statement: BaseSegment,
        context: SqlFluffAnalyzerContext,
        is_sub_query: bool = False,
    ) -> SubQueryLineageHolder:

        handlers, conditional_handlers = self._init_handlers()

        holder = self._init_holder(context)

        subqueries = []
        select_statements = []
        segments = retrieve_segments(statement)
        for segment in segments:
            for sq in self.parse_subquery(segment):
                # Collecting subquery on the way, hold on parsing until last
                # so that each handler don't have to worry about what's inside subquery
                subqueries.append(sq)

            for current_handler in handlers:
                current_handler.handle(segment, holder)

            if segment.type == "select_statement":
                select_statements.append(segment)
                continue

            if segment.type == "set_expression":
                sub_segments = retrieve_segments(segment)
                for sub_segment in sub_segments:
                    if sub_segment.type == "select_statement":
                        select_statements.append(sub_segment)
                continue

            for conditional_handler in conditional_handlers:
                if conditional_handler.indicate(segment):
                    conditional_handler.handle(segment, holder)

        # call end of query hook here as loop is over
        for conditional_handler in conditional_handlers:
            conditional_handler.end_of_query_cleanup(holder)

        # By recursively extracting each subquery of the parent and merge, we're doing Depth-first search
        for statement in select_statements:
            holder |= DmlSelectExtractor(self.dialect).extract(
                statement,
                SqlFluffAnalyzerContext(prev_cte=holder.cte, prev_write=holder.write),
            )

        for sq in subqueries:
            holder |= DmlSelectExtractor(self.dialect).extract(
                sq.segment, SqlFluffAnalyzerContext(sq, holder.cte)
            )

        return holder
