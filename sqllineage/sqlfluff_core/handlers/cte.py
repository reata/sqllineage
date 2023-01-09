from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.sqlfluff_core.handlers.base import ConditionalSegmentBaseHandler


class CTEHandler(ConditionalSegmentBaseHandler):
    """Common Table Expression (With Queries) Handler."""

    CTE_KEYWORDS = ("WITH")

    def indicate(self, segment: BaseSegment) -> bool:
        if (
            self.indicator is True
            or segment.type == "keyword"
            and segment.raw_upper in self.CTE_KEYWORDS
        ):
            self.indicator = True
            return self.indicator
        return False

    def handle(self, segment: BaseSegment, holder: SubQueryLineageHolder) -> None:
        pass
