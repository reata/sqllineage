from typing import NamedTuple, Optional, Union

from sqlfluff.core.parser.segments import BracketedSegment, BaseSegment


class SubSqlFluffQueryTuple(NamedTuple):
    bracketed: Union[BracketedSegment, BaseSegment]
    alias: Optional[str]
