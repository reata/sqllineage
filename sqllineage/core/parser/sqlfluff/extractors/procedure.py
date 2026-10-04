from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import StatementLineageHolder
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

        # recursive_crawl yields every nested statement, control-flow containers and lineage-free statements
        # (SET, PRINT, THROW, ...) included. A statement without a matching extractor is therefore the normal case here.
        # the statements that do carry lineage are yielded separately by the same crawl, so they are still picked up.
        for segment in statement.recursive_crawl("statement"):
            sub_holder = BaseExtractor.try_extract(
                self.dialect, self.metadata_provider, segment.segments[0], context
            )
            if sub_holder is not None:
                holder |= sub_holder
        return holder
