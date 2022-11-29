from sqlfluff.core.parser import BaseSegment
from sqllineage.core.models import Table
from sqllineage.utils.helpers import escape_identifier_name
from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.sqlfluff_core.handlers.base import ConditionalSegmentBaseHandler


class TargetHandler(ConditionalSegmentBaseHandler):
    """Target Table Handler"""

    def indicate(self, segment: BaseSegment) -> bool:
        if (
            self.indicator is True
            or segment.type == "keyword"
            and segment.raw_upper == "INSERT"
        ):
            self.indicator = True
            return self.indicator
        return False

    def handle(self, segment: BaseSegment, holder: SubQueryLineageHolder) -> None:
        if segment.type == "table_reference":
            holder.add_write(Table(escape_identifier_name(segment.raw)))
