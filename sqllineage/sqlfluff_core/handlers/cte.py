from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.sqlfluff_core.handlers.base import ConditionalSegmentBaseHandler


class CTEHandler(ConditionalSegmentBaseHandler):
    """Common Table Expression (With Queries) Handler."""

    def indicate(self, segment: BaseSegment) -> bool:
        return False

    def handle(self, segment: BaseSegment, holder: SubQueryLineageHolder) -> None:
        pass
