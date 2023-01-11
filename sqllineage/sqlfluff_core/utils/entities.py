from typing import NamedTuple, Optional

from sqlfluff.core.parser.segments import BracketedSegment, BaseSegment


class SubSqlFluffQueryTuple(NamedTuple):
    bracketed: BracketedSegment
    alias: Optional[str]