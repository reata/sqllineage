from abc import ABC, abstractmethod

from sqllineage.core.holders import StatementLineageHolder
from sqllineage.core.metadata_provider import MetaDataProvider


class LineageAnalyzer(ABC):
    """SQL Statement Level Lineage Analyzer
    Parser specific implementation should inherit this class and implement analyze method
    """

    PARSER_NAME: str = ""
    SUPPORTED_DIALECTS: list[str] = []

    def __init__(self, sql: str, **_):
        """
        :param sql: a string representation of single or multiple SQL statements
        """
        self._sql = sql

    @property
    @abstractmethod
    def statements(self) -> list[str]:
        """
        Split the raw SQL into individual statements.
        :return: list of individual statement strings
        """
        raise NotImplementedError

    @abstractmethod
    def analyze(
        self, sql: str, metadata_provider: MetaDataProvider
    ) -> StatementLineageHolder:
        """
        to analyze single statement sql and store the result into StatementLineageHolder.

        :param sql: single-statement SQL string to be processed
        :param metadata_provider: :class:`sqllineage.core.metadata_provider.MetaDataProvider` provides metadata on
                                  tables to help lineage analyzing
        :return: :class:`sqllineage.core.holders.StatementLineageHolder`
        """
        raise NotImplementedError
