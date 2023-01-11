from sqlfluff.core.parser import BaseSegment
from sqllineage.core.holders import SubQueryLineageHolder

from sqllineage.core.analyzer import AnalyzerContext
from sqllineage.sqlfluff_core.subquery.dml_select_extractor import DmlSelectExtractor

from sqllineage.sqlfluff_core.subquery.lineage_holder_extractor import (
    LineageHolderExtractor,
)
from sqllineage.sqlfluff_core.models import SqlFluffSubQuery
from sqllineage.sqlfluff_core.utils.sqlfluff import retrieve_segments


class NoopExtractor(LineageHolderExtractor):

    NOOP_STMT_TYPES = [
        "delete_statement",
        "truncate_table",
        "refresh_statement",
        "cache_table",
        "uncache_table",
        "show_statement",
        "use_statement",
    ]

    def can_extract(self, statement_type: str):
        return statement_type in self.NOOP_STMT_TYPES

    def extract(
        self,
        statement: BaseSegment,
        context: AnalyzerContext,
        is_sub_query: bool = False,
    ) -> None:
        return None
