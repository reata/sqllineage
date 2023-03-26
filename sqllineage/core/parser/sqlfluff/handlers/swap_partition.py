from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.parser.sqlfluff.handlers.base import SegmentBaseHandler
from sqllineage.core.parser.sqlfluff.models import SqlFluffTable
from sqllineage.core.parser.sqlfluff.utils import (
    get_grandchild,
    get_grandchildren,
)
from sqllineage.utils.helpers import escape_identifier_name


class SwapPartitionHandler(SegmentBaseHandler):
    """
    A handler for swap_partitions_between_tables function
    """

    def handle(self, segment: BaseSegment, holder: SubQueryLineageHolder) -> None:
        """
        Handle the segment, and update the lineage result accordingly in the holder
        :param segment: segment to be handled
        :param holder: 'SqlFluffSubQueryLineageHolder' to hold lineage
        """
        function_from_select = get_grandchild(
            segment, "select_clause_element", "function"
        )
        if (
            function_from_select
            and function_from_select.first_non_whitespace_segment_raw_upper
            == "SWAP_PARTITIONS_BETWEEN_TABLES"
        ):
            function_parameters = get_grandchildren(
                function_from_select, "bracketed", "expression"
            )
            holder.add_read(
                SqlFluffTable(escape_identifier_name(function_parameters[0].raw))
            )
            holder.add_write(
                SqlFluffTable(escape_identifier_name(function_parameters[3].raw))
            )
