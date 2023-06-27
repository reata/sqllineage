from typing import Optional

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
from sqllineage.core.parser.sqlfluff.utils import get_child, is_union, retrieve_segments


class DmlInsertExtractor(LineageHolderExtractor):
    """
    DML Insert queries lineage extractor
    """

    SUPPORTED_STMT_TYPES = [
        "insert_statement",
        "create_table_statement",
        "create_table_as_statement",
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

        segments = retrieve_segments(statement)
        for segment in segments:
            for current_handler in handlers:
                current_handler.handle(segment, holder)

            if segment.type == "with_compound_statement":
                from .cte_extractor import DmlCteExtractor

                holder |= DmlCteExtractor(self.dialect).extract(
                    segment,
                    AnalyzerContext(prev_cte=holder.cte, prev_write=holder.write),
                )
            elif segment.type == "bracketed" and any(
                s.type == "with_compound_statement" for s in segment.segments
            ):
                for sgmt in segment.segments:
                    if sgmt.type == "with_compound_statement":
                        from .cte_extractor import DmlCteExtractor

                        holder |= DmlCteExtractor(self.dialect).extract(
                            sgmt,
                            AnalyzerContext(
                                prev_cte=holder.cte, prev_write=holder.write
                            ),
                        )
            elif segment.type == "bracketed" and (
                self.parse_subquery(segment) or is_union(segment)
            ):
                # note regular subquery within SELECT statement is handled by DmlSelectExtractor, this is only to handle
                # top-level subquery in DML like: 1) create table foo as (subquery); 2) insert into foo (subquery)
                # subquery here isn't added as read source, and it inherits DML-level target_columns if parsed
                if subquery_segment := get_child(segment, "select_statement"):
                    self._extract_select(holder, subquery_segment)
                elif subquery_segment := get_child(segment, "set_expression"):
                    self._extract_set(holder, subquery_segment)
            elif segment.type == "select_statement":
                self._extract_select(holder, segment)
            elif segment.type == "set_expression":
                self._extract_set(holder, segment)
            else:
                for conditional_handler in conditional_handlers:
                    if conditional_handler.indicate(segment):
                        conditional_handler.handle(segment, holder)

        return holder

    def _extract_set(self, holder: SubQueryLineageHolder, set_segment: BaseSegment):
        for sub_segment in retrieve_segments(set_segment):
            if sub_segment.type == "select_statement":
                self._extract_select(holder, sub_segment, set_segment)

    def _extract_select(
        self,
        holder: SubQueryLineageHolder,
        select_segment: BaseSegment,
        set_segment: Optional[BaseSegment] = None,
    ):
        holder |= DmlSelectExtractor(self.dialect).extract(
            select_segment,
            AnalyzerContext(
                SqlFluffSubQuery.of(
                    set_segment if set_segment else select_segment, None
                ),
                prev_cte=holder.cte,
                prev_write=holder.write,
                target_columns=holder.target_columns,
            ),
        )
