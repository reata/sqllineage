from typing import NamedTuple, Optional

from sqlparse.sql import Parenthesis


class SubQueryTuple(NamedTuple):
    parenthesis: Parenthesis
    alias: str


class ColumnQualifierTuple(NamedTuple):
    column: str
    qualifier: Optional[str]
