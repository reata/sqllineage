from typing import NamedTuple, Optional

from sqlfluff.core.parser.segments import BracketedSegment


class SubSqlFluffQueryTuple(NamedTuple):
    bracketed: BracketedSegment
    alias: Optional[str]
