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
        cte: Optional[Set[SubQuery]] = None,
        write: Optional[Set[Union[SubQuery, Table]]] = None,
        write_columns=None,
    ):
        """
        :param cte: CTE queries that can be select from in current query context
        :param write: table that current top-level query is writing to, subquery in case of subquery context
        :param write_columns: columns that write table specifies, used for `INSERT INTO x (col1, col2) SELECT` syntax
        """
        self.cte = cte
        self.write = write
        if write_columns is None:
            write_columns = []
        self.write_columns = write_columns
