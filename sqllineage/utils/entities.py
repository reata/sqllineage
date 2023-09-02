from typing import Any, List, NamedTuple, Optional, Set, Union

from sqllineage.core.models import Column, SubQuery, Table


class SubQueryTuple(NamedTuple):
    parenthesis: Any
    alias: Optional[str]


class ColumnQualifierTuple(NamedTuple):
    column: str
    qualifier: Optional[str]


class AnalyzerContext(NamedTuple):
    # CTE queries that can be select from in current query context
    cte: Optional[Set[SubQuery]] = None
    # table that current top-level query is writing to, subquery in case of subquery context
    write: Optional[Set[Union[SubQuery, Table]]] = None
    # columns that write table specifies, used for `INSERT INTO x (col1, col2) SELECT` syntax
    write_columns: Optional[List[Column]] = None
