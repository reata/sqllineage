from abc import abstractmethod
from typing import List

from sqllineage.core.holders import StatementLineageHolder


class LineageAnalyzer:
    """SQL Statement Level Lineage Analyzer
    Parser specific implementation should inherit this class and implement analyze method
    """

    PARSER_NAME: str = ""
    SUPPORTED_DIALECTS: List[str] = []

    @abstractmethod
    def analyze(self, sql: str) -> StatementLineageHolder:
        """
        to analyze single statement sql and store the result into
        :class:`sqllineage.core.holders.StatementLineageHolder`.
        """
