from sqlfluff.core.parser import BaseSegment

from sqllineage.sqlfluff_core.holders import SqlFluffSubQueryLineageHolder
from sqllineage.sqlfluff_core.models import SqlFluffAnalyzerContext, SqlFluffSubQuery
from sqllineage.sqlfluff_core.subquery.dml_select_extractor import DmlSelectExtractor
from sqllineage.sqlfluff_core.subquery.lineage_holder_extractor import (
    LineageHolderExtractor,
)
from sqllineage.sqlfluff_core.utils.sqlfluff import retrieve_segments


class DmlInsertExtractor(LineageHolderExtractor):
    """
    DML Insert queries lineage extractor
    """

    DML_INSERT_STMT_TYPES = [
        "insert_statement",
        "create_table_statement",
        "create_view_statement",
        "update_statement",
        "copy_statement",
        "insert_overwrite_directory_hive_fmt_statement",
        "copy_into_statement",
        "copy_into_table_statement",
    ]

    def __init__(self, dialect: str):
        super().__init__(dialect)

    def can_extract(self, statement_type: str) -> bool:
        """
        Determine if the current lineage holder extractor can process the statement
        :param statement_type: a sqlfluff segment type
        """
        return statement_type in self.DML_INSERT_STMT_TYPES

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

        subqueries = []
        segments = retrieve_segments(statement)
        for segment in segments:
            for sq in self.parse_subquery(segment):
                # Collecting subquery on the way, hold on parsing until last
                # so that each handler don't have to worry about what's inside subquery
                subqueries.append(sq)

            for current_handler in handlers:
                current_handler.handle(segment, holder)

            if segment.type == "select_statement":
                holder |= DmlSelectExtractor(self.dialect).extract(
                    segment,
                    SqlFluffAnalyzerContext(
                        SqlFluffSubQuery(segment, None),
                        prev_cte=holder.cte,
                        prev_write=holder.write,
                    ),
                )
                continue

            if segment.type == "set_expression":
                sub_segments = retrieve_segments(segment)
                for sub_segment in sub_segments:
                    if sub_segment.type == "select_statement":
                        holder |= DmlSelectExtractor(self.dialect).extract(
                            sub_segment,
                            SqlFluffAnalyzerContext(
                                SqlFluffSubQuery(segment, None),
                                prev_cte=holder.cte,
                                prev_write=holder.write,
                            ),
                        )
                continue

            for conditional_handler in conditional_handlers:
                if conditional_handler.indicate(segment):
                    conditional_handler.handle(segment, holder)

        # By recursively extracting each subquery of the parent and merge, we're doing Depth-first search
        for sq in subqueries:
            holder |= DmlSelectExtractor(self.dialect).extract(
                sq.segment, SqlFluffAnalyzerContext(sq, holder.cte)
            )

        return holder
