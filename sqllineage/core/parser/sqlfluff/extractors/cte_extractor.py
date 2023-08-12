from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.parser.sqlfluff.extractors.dml_insert_extractor import (
    DmlInsertExtractor,
)
from sqllineage.core.parser.sqlfluff.extractors.dml_select_extractor import (
    DmlSelectExtractor,
)
from sqllineage.core.parser.sqlfluff.extractors.lineage_holder_extractor import (
    LineageHolderExtractor,
)
from sqllineage.core.parser.sqlfluff.models import SqlFluffSubQuery
from sqllineage.core.parser.sqlfluff.utils import get_children, list_child_segments
from sqllineage.utils.entities import AnalyzerContext


class DmlCteExtractor(LineageHolderExtractor):
    """
    DML CTE queries lineage extractor
    """

    SUPPORTED_STMT_TYPES = ["with_compound_statement"]

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
        holder = self._init_holder(context)
        subqueries = []
        for segment in list_child_segments(statement):
            if segment.type in ["select_statement", "set_expression"]:
                holder |= DmlSelectExtractor(self.dialect).extract(
                    segment,
                    AnalyzerContext(cte=holder.cte, write=holder.write),
                )

            if segment.type == "insert_statement":
                holder |= DmlInsertExtractor(self.dialect).extract(
                    segment,
                    AnalyzerContext(cte=holder.cte),
                )

            identifier = None
            if segment.type == "common_table_expression":
                segment_has_alias = any(
                    s for s in get_children(segment, "keyword") if s.raw_upper == "AS"
                )
                sub_segments = list_child_segments(segment)
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

        # By recursively extracting each extractor of the parent and merge, we're doing Depth-first search
        for sq in subqueries:
            holder |= DmlSelectExtractor(self.dialect).extract(
                sq.query,
                AnalyzerContext(cte=holder.cte, write={sq}),
            )

        return holder
