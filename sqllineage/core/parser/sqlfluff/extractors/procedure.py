import warnings

from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import StatementLineageHolder, SubQueryLineageHolder
from sqllineage.core.parser.sqlfluff.extractors.base import BaseExtractor
from sqllineage.utils.entities import AnalyzerContext


class ProcedureExtractor(BaseExtractor):
    """
    Stored Procedure lineage extractor.
    """

    SUPPORTED_STMT_TYPES = ["create_procedure_statement"]

    def extract(
        self,
        statement: BaseSegment,
        context: AnalyzerContext,
    ) -> StatementLineageHolder:
        holder = StatementLineageHolder()

        for segment in statement.recursive_crawl("statement"):
            holder |= self._delegate_to_extractor(segment.segments[0], context)
        return holder

    def _delegate_to_extractor(
        self, segment: BaseSegment, context: AnalyzerContext
    ) -> SubQueryLineageHolder:
        holder = BaseExtractor.try_extract(
            self.dialect, self.metadata_provider, segment, context
        )
        if holder is not None:
            return holder
        else:
            warnings.warn(
                "SQLLineage doesn't support analyzing statement type "
                f"[{segment.type}] for SQL Segment: '{segment.raw}' embedding in procedure, skipping it."
            )
            return SubQueryLineageHolder()
