from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.models import Path, Schema, Table, Column
from sqllineage.core.parser.sqlfluff.extractors.base import BaseExtractor
from sqllineage.core.parser.sqlfluff.extractors.select import SelectExtractor
from sqllineage.core.parser.sqlfluff.models import SqlFluffColumn, SqlFluffTable
from sqllineage.core.parser.sqlfluff.utils import (
    is_set_expression,
    list_child_segments,
)
from sqllineage.utils.entities import AnalyzerContext, ColumnQualifierTuple
from sqllineage.utils.helpers import escape_identifier_name


class CreateInsertExtractor(BaseExtractor):
    """
    Create statement and Insert statement lineage extractor
    """

    SUPPORTED_STMT_TYPES = [
        "create_table_statement",
        "create_table_as_statement",
        "create_view_statement",
        "insert_statement",
        "insert_overwrite_directory_hive_fmt_statement",
    ]

    def extract(
            self,
            statement: BaseSegment,
            context: AnalyzerContext,
    ) -> SubQueryLineageHolder:
        holder = self._init_holder(context)
        src_flag = tgt_flag = False
        statement = list_child_segments(statement)
        for seg_idx, segment in enumerate(statement):
            if segment.type == "with_compound_statement":
                holder |= self.delegate_to_cte(segment, holder)
            elif segment.type == "bracketed" and any(
                    s.type == "with_compound_statement" for s in segment.segments
            ):
                for sgmt in segment.segments:
                    if sgmt.type == "with_compound_statement":
                        holder |= self.delegate_to_cte(segment, holder)
            elif segment.type in ("select_statement", "set_expression"):
                holder |= self.delegate_to_select(segment, holder)
            elif segment.type == "values_clause":
                for bracketed in segment.get_children("bracketed"):
                    for expression in bracketed.get_children("expression"):
                        if sub_bracketed := expression.get_child("bracketed"):
                            if sub_expression := sub_bracketed.get_child("expression"):
                                if select_statement := sub_expression.get_child(
                                        "select_statement"
                                ):
                                    holder |= self.delegate_to_select(
                                        select_statement, holder
                                    )
            elif segment.type == "bracketed" and (
                    self.list_subquery(segment) or is_set_expression(segment)
            ):
                # note regular subquery within SELECT statement is handled by SelectExtractor, this is only to handle
                # top-level subquery in DML like: 1) create table foo as (subquery); 2) insert into foo (subquery)
                # subquery here isn't added as read source, and it inherits DML-level write_columns if parsed
                if subquery_segment := segment.get_child(
                        "select_statement", "set_expression"
                ):
                    holder |= self.delegate_to_select(subquery_segment, holder)

            elif segment.type == "bracketed":
                # In case of bracketed column reference, add these target columns to holder
                # so that when we compute the column level lineage
                # we keep these columns into consideration
                sub_segments = list_child_segments(segment)
                if all(
                        sub_segment.type in ["column_reference", "column_definition"]
                        for sub_segment in sub_segments
                ):
                    # target columns only apply to bracketed column_reference and column_definition
                    columns = []
                    for sub_segment in sub_segments:
                        if sub_segment.type == "column_definition":
                            if identifier := sub_segment.get_child("identifier"):
                                sub_segment = identifier
                        columns.append(SqlFluffColumn.of(sub_segment))
                    holder.add_write_column(*columns)

            elif segment.type == "keyword":
                if segment.raw_upper in [
                    "INTO",
                    "OVERWRITE",
                    "TABLE",
                    "VIEW",
                    "DIRECTORY",
                ] or (
                        tgt_flag is True and segment.raw_upper in ["IF", "NOT", "EXISTS"]
                ):
                    tgt_flag = True
                elif segment.raw_upper in ["LIKE", "CLONE"]:
                    src_flag = True
                continue

            if tgt_flag:
                if segment.type in ["table_reference", "object_reference"]:
                    write_obj = SqlFluffTable.of(segment)
                    holder.add_write(write_obj)
                elif segment.type == "literal":
                    if segment.raw.isnumeric():
                        # Special Handling for Spark Bucket Table DDL
                        pass
                    else:
                        holder.add_write(Path(escape_identifier_name(segment.raw)))
                tgt_flag = False

                if statement[0].raw_upper == 'INSERT' and holder.write:
                    sub_segments = list_child_segments(statement[seg_idx + 1])
                    if not all(
                            sub_segment.type in ["column_reference", "column_definition"]
                            for sub_segment in sub_segments
                    ):
                        tgt_tab = list(holder.write)[0]
                        if isinstance(tgt_tab, Table) and (self.default_schema or tgt_tab.schema.raw_name != Schema.unknown):
                            schema = tgt_tab.schema if tgt_tab.schema.raw_name != Schema.unknown else Schema(self.default_schema)
                            col_list = [Column(col.raw_name, source_columns=[ColumnQualifierTuple(column=col.raw_name, qualifier=None)])
                                        for col in self.metadata_provider.get_table_columns(Table(tgt_tab.raw_name, schema))]
                            holder.add_write_column(*col_list)
                    # is_find_table_reference = False
                    # for seg_idx, seg in enumerate(statement.segments):
                    #     if seg.type == 'whitespace':
                    #         continue
                    #     if seg.type == 'table_reference':
                    #         is_find_table_reference = True
                    #         continue
                    #     if is_find_table_reference:
                    #         if seg.type != 'bracketed' or not (
                    #                 seg.type == 'bracketed' and
                    #                 not set([bracketed_seg.type for bracketed_seg in seg.segments]).difference(
                    #                     {'symbol', 'indent', 'whitespace', 'column_reference', 'dedent'})):
                    #             tgt_tab = list(holder.write)[0]
                    #             if isinstance(tgt_tab, Table) and (self.default_schema or tgt_tab.schema.raw_name != Schema.unknown):
                    #                 schema = tgt_tab.schema if tgt_tab.schema.raw_name != Schema.unknown else Schema(self.default_schema)
                    #                 col_list = [Column(col.raw_name, source_columns=[ColumnQualifierTuple(column=col.raw_name, qualifier=None)])
                    #                             for col in self.metadata_provider.get_table_columns(Table(tgt_tab.raw_name, schema))]
                    #                 holder.add_write_column(*col_list)
                    #         break
            if src_flag:
                if segment.type in ["table_reference", "object_reference"]:
                    holder.add_read(SqlFluffTable.of(segment))
                src_flag = False
        return holder

    def delegate_to_cte(
            self, segment: BaseSegment, holder: SubQueryLineageHolder
    ) -> SubQueryLineageHolder:
        from .cte import CteExtractor

        return self.delegate_to(
            CteExtractor,
            segment,
            AnalyzerContext(
                cte=holder.cte, write=holder.write, write_columns=holder.write_columns
            ),
        )

    def delegate_to_select(
            self,
            segment: BaseSegment,
            holder: SubQueryLineageHolder,
    ) -> SubQueryLineageHolder:
        return self.delegate_to(
            SelectExtractor,
            segment,
            AnalyzerContext(
                cte=holder.cte,
                write=holder.write,
                write_columns=holder.write_columns,
            ),
        )
