from typing import NamedTuple, Optional, Union

from sqlfluff.core.parser.segments import BaseSegment


class SubSqlFluffQueryTuple(NamedTuple):
    """
    Tuple of segment and optional alias
    """

    bracketed: Union[BaseSegment]
    alias: Optional[str]


class SqlFluffColumnQualifierTuple(NamedTuple):
    """
    Tuple of column name and qualifier
    """

    column: str
    qualifier: Optional[str]
