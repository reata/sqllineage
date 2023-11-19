from abc import abstractmethod
from typing import List

from sqllineage.core.holders import StatementLineageHolder
from sqllineage.core.metadata_provider import MetaDataProvider


class LineageAnalyzer:
    """SQL Statement Level Lineage Analyzer
    Parser specific implementation should inherit this class and implement analyze method
    """

    PARSER_NAME: str = ""
    SUPPORTED_DIALECTS: List[str] = []

    @abstractmethod
    def analyze(
        self,
        sql: str,
        pre_stmt_holders: List[StatementLineageHolder],
        metadata_provider: MetaDataProvider,
        silent_mode: bool,
    ) -> StatementLineageHolder:
        """
        to analyze single statement sql and store the result into
        :class:`sqllineage.core.holders.StatementLineageHolder`

        :param silent_mode: skip unsupported statements.
        """
