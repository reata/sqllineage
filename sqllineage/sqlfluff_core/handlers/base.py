from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import SubQueryLineageHolder


class ConditionalSegmentBaseHandler:
    """
    This is to address an extract pattern when a specified segment match the condition
    """

    def __init__(self, dialect: str) -> None:
        self.indicator = False
        self.dialect = dialect

    def handle(self, segment: BaseSegment, holder: SubQueryLineageHolder) -> None:
        """
        Handle the indicated token, and update the lineage result accordingly
        """
        raise NotImplementedError

    def indicate(self, segment: BaseSegment) -> bool:
        """
        Set indicator to True only when _indicate returns True
        """
        return False

    def end_of_query_cleanup(self, holder: SubQueryLineageHolder) -> None:
        """
        Optional hook to be called at the end of statement or subquery
        """
        pass


class SegmentBaseHandler:
    """
    This is to address an extract pattern when we should extract something from current token
    """

    def handle(self, segment: BaseSegment, holder: SubQueryLineageHolder) -> None:
        raise NotImplementedError
