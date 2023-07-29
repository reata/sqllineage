from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import SubQueryLineageHolder


class ConditionalSegmentBaseHandler:
    """
    Extract lineage from a segment when the segment match the condition
    """

    def handle(self, segment: BaseSegment, holder: SubQueryLineageHolder) -> None:
        """
        Handle the segment, and update the lineage result accordingly in the holder
        :param segment: segment to be handled
        :param holder: 'SubQueryLineageHolder' to hold lineage
        """
        raise NotImplementedError

    def indicate(self, segment: BaseSegment) -> bool:
        """
        Indicates if the handler can handle the segment
        :param segment: segment to be handled
        :return: True if it can be handled, by default return False
        """
        raise NotImplementedError

    def end_of_query_cleanup(self, holder: SubQueryLineageHolder) -> None:
        """
        Optional method to be called at the end of statement or subquery
        :param holder: 'SubQueryLineageHolder' to hold lineage
        """
        pass
