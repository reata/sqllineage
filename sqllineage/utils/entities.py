from typing import Any, NamedTuple, Optional, Set, Union

from sqllineage.core.models import SubQuery, Table


class SubQueryTuple(NamedTuple):
    parenthesis: Any
    alias: Optional[str]


class ColumnQualifierTuple(NamedTuple):
    column: str
    qualifier: Optional[str]


class AnalyzerContext:
    """
    Data class to hold the analyzer context
    """

    def __init__(
        self,
        prev_cte: Optional[Set[SubQuery]] = None,
        prev_write: Optional[Set[Union[SubQuery, Table]]] = None,
        target_columns=None,
    ):
        """
        :param prev_cte: previous CTE queries
        :param prev_write: previous written tables
        :param target_columns: previous target columns
        """
        if target_columns is None:
            target_columns = []
        self.prev_cte = prev_cte
        self.prev_write = prev_write
        self.target_columns = target_columns
