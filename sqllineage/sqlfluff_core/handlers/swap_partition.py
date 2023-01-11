from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.sqlfluff_core.handlers.base import SegmentBaseHandler
from sqllineage.sqlfluff_core.models import SqlFluffTable
from sqllineage.utils.helpers import escape_identifier_name


class SwapPartitionHandler(SegmentBaseHandler):
    """
    a special handling for swap_partitions_between_tables function of Vertica SQL dialect.
    """

    def handle(self, segment: BaseSegment, holder: SubQueryLineageHolder) -> None:
        if segment.get_child("select_clause_element"):
            function_from_select = segment.get_child("select_clause_element").get_child(
                "function"
            )
            if (
                function_from_select
                and function_from_select.first_non_whitespace_segment_raw_upper.lower()
                == "swap_partitions_between_tables"
            ):
                function_parameters = function_from_select.get_child(
                    "bracketed"
                ).get_children("expression")
                holder.add_read(
                    SqlFluffTable(escape_identifier_name(function_parameters[0].raw))
                )
                holder.add_write(
                    SqlFluffTable(escape_identifier_name(function_parameters[3].raw))
                )
