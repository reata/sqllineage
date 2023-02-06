from sqlfluff.core.parser import BaseSegment

from sqllineage.holders import SubQueryLineageHolder
from sqllineage.models import AnalyzerContext
from sqllineage.sqlfluff_core.subquery.lineage_holder_extractor import (
    LineageHolderExtractor,
)


class NoopExtractor(LineageHolderExtractor):
    """
    Extractor for queries which do not provide any lineage
    """

    NOOP_STMT_TYPES = [
        "delete_statement",
        "truncate_table",
        "refresh_statement",
        "cache_table",
        "uncache_table",
        "show_statement",
        "use_statement",
    ]

    def __init__(self, dialect: str):
        super().__init__(dialect)

    def can_extract(self, statement_type: str) -> bool:
        """
        Determine if the current lineage holder extractor can process the statement
        :param statement_type: a sqlfluff segment type
        """
        return statement_type in self.NOOP_STMT_TYPES

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
        return SubQueryLineageHolder()
