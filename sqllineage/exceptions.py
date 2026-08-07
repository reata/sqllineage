from typing import Any


class SQLLineageException(Exception):
    """Base Exception for SQLLineage"""


class UnsupportedStatementException(SQLLineageException):
    """Raised for SQL statement that SQLLineage doesn't support analyzing"""


class InvalidSyntaxException(SQLLineageException):
    """Raised for SQL statement that parser cannot parse"""


class MetaDataProviderException(SQLLineageException):
    """Raised for MetaDataProvider errors"""


class ConfigException(SQLLineageException):
    """Raised for configuration errors"""


class AmbiguousNode(SQLLineageException):
    """Raised when a node filter matches more than one column"""

    def __init__(self, message: str, matches: list[Any]):
        super().__init__(message)
        self.matches = matches
