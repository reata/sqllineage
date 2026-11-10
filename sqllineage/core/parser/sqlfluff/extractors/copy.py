from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.models import Path
from sqllineage.core.parser.sqlfluff.extractors.base import BaseExtractor
from sqllineage.core.parser.sqlfluff.extractors.cte import CteExtractor
from sqllineage.core.parser.sqlfluff.extractors.select import SelectExtractor
from sqllineage.core.parser.sqlfluff.models import SqlFluffColumn
from sqllineage.core.parser.sqlfluff.utils import (
    find_from_expression_element,
    list_child_segments,
)
from sqllineage.utils.entities import AnalyzerContext
from sqllineage.utils.helpers import escape_identifier_name


class CopyExtractor(BaseExtractor):
    """
    Copy statement lineage extractor
    """

    SUPPORTED_STMT_TYPES = [
        "copy_statement",
        "copy_into_table_statement",
        "copy_into_location_statement",
    ]

    def extract(
        self, statement: BaseSegment, context: AnalyzerContext
    ) -> SubQueryLineageHolder:
        holder = self._init_holder(context)
        src_flag = tgt_flag = False
        for segment in list_child_segments(statement):
            if segment.type == "from_clause":
                if from_expression_element := find_from_expression_element(segment):
                    for table_expression in from_expression_element.get_children(
                        "table_expression"
                    ):
                        if storage_location := table_expression.get_child(
                            "storage_location"
                        ):
                            holder.add_read(Path(storage_location.raw))
            elif segment.type == "bracketed" and (
                query_segments := list(
                    segment.recursive_crawl(
                        "select_statement",
                        "set_expression",
                        "with_compound_statement",
                        recurse_into=False,
                    )
                )
            ):
                # snowflake transforms data while loading or unloading with a subquery:
                #   COPY INTO tab1 FROM (SELECT $1, $2 FROM @mystage/path/)
                #   COPY INTO @mystage/path/ FROM (SELECT col1, col2 FROM tab1)
                for query_segment in query_segments:
                    holder |= self._delegate_to_query(query_segment, holder)
            elif segment.type == "bracketed":
                # target column list: COPY INTO tab1 (col1, col2) FROM ...
                sub_segments = list_child_segments(segment)
                if all(
                    sub_segment.type == "column_reference"
                    for sub_segment in sub_segments
                ):
                    holder.add_write_column(
                        *[
                            SqlFluffColumn.of(sub_segment)
                            for sub_segment in sub_segments
                        ]
                    )
            elif segment.type == "keyword":
                if segment.raw_upper in ["COPY", "INTO"]:
                    tgt_flag = True
                elif segment.raw_upper == "FROM":
                    src_flag = True
                continue

            if tgt_flag:
                if path := self._find_path(segment):
                    holder.add_write(path)
                elif table := self.find_table(segment):
                    holder.add_write(table)
                tgt_flag = False
            if src_flag:
                if path := self._find_path(segment):
                    holder.add_read(path)
                elif table := self.find_table(segment):
                    holder.add_read(table)
                src_flag = False

        return holder

    @staticmethod
    def _find_path(segment: BaseSegment) -> Path | None:
        """
        A quoted file path, a snowflake storage location (stage path or bucket path) or a stage path that sqlfluff
        wraps in table_reference, like: COPY INTO @mystage/path/ FROM (SELECT col1 FROM tab1)
        """
        if segment.type in ["literal", "storage_location"]:
            return Path(escape_identifier_name(segment.raw))
        elif stage_path := segment.get_child("stage_path"):
            return Path(escape_identifier_name(stage_path.raw))
        return None

    def _delegate_to_query(
        self, segment: BaseSegment, holder: SubQueryLineageHolder
    ) -> SubQueryLineageHolder:
        extractor_cls = (
            CteExtractor
            if segment.type == "with_compound_statement"
            else SelectExtractor
        )
        return self.delegate_to(
            extractor_cls,
            segment,
            AnalyzerContext(
                cte=holder.cte,
                write=holder.write,
                write_columns=holder.write_columns,
            ),
        )
