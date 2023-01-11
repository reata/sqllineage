from sqlfluff.core.parser import BaseSegment
from sqllineage.core.models import Path
from sqllineage.sqlfluff_core.models import SqlFluffTable
from sqllineage.utils.helpers import escape_identifier_name
from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.sqlfluff_core.handlers.base import ConditionalSegmentBaseHandler


class TargetHandler(ConditionalSegmentBaseHandler):
    """Target Table Handler"""

    def __init__(self) -> None:
        super().__init__()
        self.prev_token_like = False
        self.prev_token_from = False

    TARGET_KEYWORDS = (
        "INTO",
        "OVERWRITE",
        "TABLE",
        "VIEW",
        "UPDATE",
        "COPY",
        "DIRECTORY",
    )

    LIKE_KEYWORD = "LIKE"

    FROM_KEYWORD = "FROM"

    def _init_tokens(self, segment: BaseSegment) -> None:
        if segment.raw_upper == self.LIKE_KEYWORD:
            self.prev_token_like = True

        if segment.raw_upper == self.FROM_KEYWORD:
            self.prev_token_from = True

    def _reset_tokens(self) -> None:
        self.prev_token_like = False
        self.prev_token_from = False

    def indicate(self, segment: BaseSegment) -> bool:
        if (
            self.indicator is True
            or segment.type == "keyword"
            and segment.raw_upper in self.TARGET_KEYWORDS
        ):
            self.indicator = True
            self._init_tokens(segment)
            return self.indicator
        return False

    def handle(self, segment: BaseSegment, holder: SubQueryLineageHolder) -> None:
        if segment.type == "table_reference":
            if self.prev_token_like:
                holder.add_read(SqlFluffTable(escape_identifier_name(segment.raw)))
            else:
                holder.add_write(SqlFluffTable(escape_identifier_name(segment.raw)))
            self._reset_tokens()

        elif segment.type in {"literal", "storage_location"}:
            if self.prev_token_from:
                holder.add_read(Path(escape_identifier_name(segment.raw)))
            else:
                holder.add_write(Path(escape_identifier_name(segment.raw)))
            self._reset_tokens()
