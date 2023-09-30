from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.parser.sqlfluff.extractors.base import BaseExtractor
from sqllineage.core.parser.sqlfluff.extractors.create_insert import (
    CreateInsertExtractor,
)
from sqllineage.core.parser.sqlfluff.extractors.select import SelectExtractor
from sqllineage.core.parser.sqlfluff.models import SqlFluffSubQuery
from sqllineage.core.parser.sqlfluff.utils import list_child_segments
from sqllineage.utils.entities import AnalyzerContext


class CteExtractor(BaseExtractor):
    """
    CTE queries lineage extractor
    """

    SUPPORTED_STMT_TYPES = ["with_compound_statement"]

    def extract(
        self,
        statement: BaseSegment,
        context: AnalyzerContext,
    ) -> SubQueryLineageHolder:
        holder = self._init_holder(context)
        subqueries = []
        for segment in list_child_segments(statement):
            if segment.type in ["select_statement", "set_expression"]:
                holder |= self.delegate_to(
                    SelectExtractor,
                    segment,
                    AnalyzerContext(cte=holder.cte, write=holder.write),
                )
            elif segment.type == "insert_statement":
                holder |= self.delegate_to(
                    CreateInsertExtractor, segment, AnalyzerContext(cte=holder.cte)
                )
            elif segment.type == "common_table_expression":
                identifier = None
                segment_has_alias = any(
                    s for s in segment.get_children("keyword") if s.raw_upper == "AS"
                )
                sub_segments = list_child_segments(segment)
                for sub_segment in sub_segments:
                    if sub_segment.type == "identifier":
                        identifier = sub_segment.raw
                        if not segment_has_alias:
                            holder.add_cte(SqlFluffSubQuery.of(sub_segment, identifier))
                    if sub_segment.type == "bracketed":
                        for sq in self.list_subquery(sub_segment):
                            if identifier:
                                sq.alias = identifier
                            subqueries.append(sq)
                        if segment_has_alias:
                            holder.add_cte(SqlFluffSubQuery.of(sub_segment, identifier))

        # By recursively extracting each extractor of the parent and merge, we're doing Depth-first search
        for sq in subqueries:
            holder |= SelectExtractor(self.dialect).extract(
                sq.query,
                AnalyzerContext(cte=holder.cte, write={sq}),
            )

        return holder
