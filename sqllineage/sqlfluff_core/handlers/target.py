from typing import Optional, Union

from sqlfluff.core.parser import BaseSegment

from sqllineage.sqlfluff_core.handlers.base import ConditionalSegmentBaseHandler
from sqllineage.sqlfluff_core.holders import SqlFluffSubQueryLineageHolder
from sqllineage.sqlfluff_core.models import (
    SqlFluffPath,
    SqlFluffSubQuery,
    SqlFluffTable,
)
from sqllineage.sqlfluff_core.utils.holder import retrieve_holder_data_from
from sqllineage.sqlfluff_core.utils.sqlfluff import (
    find_table_identifier,
    get_child,
    retrieve_segments,
)
from sqllineage.utils.helpers import escape_identifier_name


class TargetHandler(ConditionalSegmentBaseHandler):
    """
    Target table handler
    """

    def __init__(self, dialect: str) -> None:
        super().__init__(dialect)
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
        """
        Check if the segment is a 'LIKE' or 'FROM' keyword
        :param segment: segment to be use for checking
        """
        if segment.raw_upper == self.LIKE_KEYWORD:
            self.prev_token_like = True

        if segment.raw_upper == self.FROM_KEYWORD:
            self.prev_token_from = True

    def _reset_tokens(self) -> None:
        """
        Set 'prev_token_like' and 'prev_token_like' variable to False
        """
        self.prev_token_like = False
        self.prev_token_from = False

    def indicate(self, segment: BaseSegment) -> bool:
        """
        Indicates if the handler can handle the segment
        :param segment: segment to be processed
        :return: True if it can be handled
        """
        if (
            self.indicator is True
            or segment.type == "keyword"
            and segment.raw_upper in self.TARGET_KEYWORDS
        ):
            self.indicator = True
            self._init_tokens(segment)
            return self.indicator
        return False

    def handle(
        self, segment: BaseSegment, holder: SqlFluffSubQueryLineageHolder
    ) -> None:
        """
        Handle the segment, and update the lineage result accordingly in the holder
        :param segment: segment to be handled
        :param holder: 'SqlFluffSubQueryLineageHolder' to hold lineage
        """
        object_segment = self._extract_table_reference(segment, holder)
        if segment.type == "table_reference" or object_segment:
            write_obj = object_segment if object_segment else SqlFluffTable.of(segment)
            if self.prev_token_like:
                holder.add_read(write_obj)
            else:
                holder.add_write(write_obj)
            self._reset_tokens()

        elif segment.type in {"literal", "storage_location"}:
            if self.prev_token_from:
                holder.add_read(SqlFluffPath(escape_identifier_name(segment.raw)))
            else:
                holder.add_write(SqlFluffPath(escape_identifier_name(segment.raw)))
            self._reset_tokens()

        elif segment.type == "from_expression":
            from_expression_element = get_child(segment, "from_expression_element")
            if from_expression_element:
                table_identifier = find_table_identifier(from_expression_element)
                all_segments = [
                    seg
                    for seg in retrieve_segments(from_expression_element)
                    if seg.type != "keyword"
                ]
                if table_identifier:
                    write = retrieve_holder_data_from(
                        all_segments, holder, table_identifier
                    )
                    if write:
                        holder.add_write(write)
            join_clause = get_child(segment, "join_clause")
            if from_expression_element:
                table_identifier = find_table_identifier(join_clause)
                all_segments = [
                    seg
                    for seg in retrieve_segments(join_clause)
                    if seg.type != "keyword"
                ]
                if table_identifier:
                    read = retrieve_holder_data_from(
                        all_segments, holder, table_identifier
                    )
                    if read:
                        holder.add_read(read)

    @staticmethod
    def _extract_table_reference(
        object_reference: BaseSegment, holder: SqlFluffSubQueryLineageHolder
    ) -> Optional[Union[SqlFluffTable, SqlFluffSubQuery]]:
        """
        :param object_reference: object reference segment
        :param holder: 'SqlFluffSubQueryLineageHolder' to hold lineage
        :return: a 'SqlFluffTable' or 'SqlFluffSubQuery' from the object_reference
        """
        if object_reference and object_reference.type == "object_reference":
            return retrieve_holder_data_from(
                object_reference.segments,
                holder,
                get_child(object_reference, "identifier"),
            )
        return None
