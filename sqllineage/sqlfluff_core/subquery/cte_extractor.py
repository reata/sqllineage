from sqlfluff.core.parser import BaseSegment

from sqllineage.sqlfluff_core.holders import SqlFluffSubQueryLineageHolder
from sqllineage.sqlfluff_core.models import SqlFluffAnalyzerContext
from sqllineage.sqlfluff_core.models import SqlFluffSubQuery
from sqllineage.sqlfluff_core.subquery.dml_insert_extractor import DmlInsertExtractor
from sqllineage.sqlfluff_core.subquery.dml_select_extractor import DmlSelectExtractor
from sqllineage.sqlfluff_core.subquery.lineage_holder_extractor import (
    LineageHolderExtractor,
)
from sqllineage.sqlfluff_core.utils.sqlfluff import has_alias, retrieve_segments


class DmlCteExtractor(LineageHolderExtractor):
    """
    DML CTE queries lineage extractor
    """

    CTE_STMT_TYPES = ["with_compound_statement"]

    def __init__(self, dialect: str):
        super().__init__(dialect)

    def can_extract(self, statement_type: str) -> bool:
        """
        Determine if the current lineage holder extractor can process the statement
        :param statement_type: a sqlfluff segment type
        """
        return statement_type in self.CTE_STMT_TYPES

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
                        prev_cte=holder.cte, prev_write=holder.write
                    ),
                )

            if segment.type == "insert_statement":
                holder |= DmlInsertExtractor(self.dialect).extract(
                    segment,
                    SqlFluffAnalyzerContext(prev_cte=holder.cte),
                )

            identifier = None
            if segment.type == "common_table_expression":
                segment_has_alias = has_alias(segment)
                sub_segments = retrieve_segments(segment)
                for sub_segment in sub_segments:
                    if sub_segment.type == "identifier":
                        identifier = sub_segment.raw
                        if not segment_has_alias:
                            holder.add_cte(SqlFluffSubQuery.of(sub_segment, identifier))
                    if sub_segment.type == "bracketed":
                        for sq in self.parse_subquery(sub_segment):
                            if identifier:
                                sq.alias = identifier
                            subqueries.append(sq)
                        if segment_has_alias:
                            holder.add_cte(SqlFluffSubQuery.of(sub_segment, identifier))

            for conditional_handler in conditional_handlers:
                if conditional_handler.indicate(segment):
                    conditional_handler.handle(segment, holder)

        # By recursively extracting each subquery of the parent and merge, we're doing Depth-first search
        for sq in subqueries:
            holder |= DmlSelectExtractor(self.dialect).extract(
                sq.segment,
                SqlFluffAnalyzerContext(sq, prev_cte=holder.cte),
            )

        return holder
