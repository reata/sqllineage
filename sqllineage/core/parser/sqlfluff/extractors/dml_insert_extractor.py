from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.models import AnalyzerContext
from sqllineage.core.parser.sqlfluff.extractors.dml_select_extractor import (
    DmlSelectExtractor,
)
from sqllineage.core.parser.sqlfluff.extractors.lineage_holder_extractor import (
    LineageHolderExtractor,
)
from sqllineage.core.parser.sqlfluff.models import SqlFluffSubQuery
from sqllineage.core.parser.sqlfluff.utils import retrieve_segments


class DmlInsertExtractor(LineageHolderExtractor):
    """
    DML Insert queries lineage extractor
    """

    SUPPORTED_STMT_TYPES = [
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
                    AnalyzerContext(
                        SqlFluffSubQuery.of(segment, None),
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
                            AnalyzerContext(
                                SqlFluffSubQuery.of(segment, None),
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
                sq.query, AnalyzerContext(sq, holder.cte)
            )

        return holder
