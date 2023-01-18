from sqlfluff.core.parser import BaseSegment

from sqllineage.sqlfluff_core.holders import SqlFluffSubQueryLineageHolder


class ConditionalSegmentBaseHandler:
    """
    Extract lineage from a segment when the segment match the condition
    """

    def __init__(self, dialect: str) -> None:
        """
        :param dialect: dialect of the handler
        """
        self.indicator = False
        self.dialect = dialect

    def handle(
        self, segment: BaseSegment, holder: SqlFluffSubQueryLineageHolder
    ) -> None:
        """
        Handle the segment, and update the lineage result accordingly in the holder
        :param segment: segment to be handled
        :param holder: 'SqlFluffSubQueryLineageHolder' to hold lineage
        """
        raise NotImplementedError

    def indicate(self, segment: BaseSegment) -> bool:
        """
        Indicates if the handler can handle the segment
        :param segment: segment to be handled
        :return: True if it can be handled, by default return False
        """
        return False

    def end_of_query_cleanup(self, holder: SqlFluffSubQueryLineageHolder) -> None:
        """
        Optional method to be called at the end of statement or subquery
        :param holder: 'SqlFluffSubQueryLineageHolder' to hold lineage
        """
        pass


class SegmentBaseHandler:
    """
    Extract lineage from a specific segment
    """

    def handle(
        self, segment: BaseSegment, holder: SqlFluffSubQueryLineageHolder
    ) -> None:
        """

        :param segment: segment to be handled
        :param holder: 'SqlFluffSubQueryLineageHolder' to hold lineage
        """
        raise NotImplementedError
