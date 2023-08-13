from typing import List
from typing import Optional, Tuple

from sqlfluff.core.parser import BaseSegment

from sqllineage import SQLPARSE_DIALECT
from sqllineage.core.models import Column, Schema, SubQuery, Table
from sqllineage.core.parser.sqlfluff.utils import (
    extract_column_qualifier,
    extract_identifier,
    is_subquery,
    is_wildcard,
    list_child_segments,
)
from sqllineage.utils.entities import ColumnQualifierTuple
from sqllineage.utils.helpers import escape_identifier_name

NON_IDENTIFIER_OR_COLUMN_SEGMENT_TYPE = [
    "function",
    "over_clause",
    "partitionby_clause",
    "orderby_clause",
    "expression",
    "case_expression",
    "when_clause",
    "else_clause",
    "select_clause_element",
    "cast_expression",
]

SOURCE_COLUMN_SEGMENT_TYPE = NON_IDENTIFIER_OR_COLUMN_SEGMENT_TYPE + [
    "identifier",
    "column_reference",
]


class SqlFluffTable(Table):
    """
    Data Class for SqlFluffTable
    """

    @staticmethod
    def of(table: BaseSegment, alias: Optional[str] = None) -> Table:
        """
        Build an object of type 'Table'
        :param table: table segment to be processed
        :param alias: alias of the table segment
        :return: 'Table' object
        """
        dot_idx = None
        for idx in range(len(table.segments) - 2, -1, -1):
            token = table.segments[idx]
            if bool(token.type == "symbol"):
                dot_idx, _ = idx, token
                break
        real_name = (
            table.segments[dot_idx + 1].raw
            if dot_idx
            else (table.raw if table.type == "identifier" else table.segments[0].raw)
        )
        # rewrite identifier's get_parent_name accordingly
        parent_name = (
            "".join(
                [
                    escape_identifier_name(segment.raw)
                    for segment in table.segments[:dot_idx]
                ]
            )
            if dot_idx
            else None
        )
        schema = Schema(parent_name) if parent_name is not None else Schema()
        kwargs = {"alias": alias} if alias else {}
        return Table(real_name, schema, **kwargs)


class SqlFluffSubQuery(SubQuery):
    """
    Data Class for SqlFluffSubQuery
    """

    @staticmethod
    def of(subquery: BaseSegment, alias: Optional[str]) -> SubQuery:
        """
        Build a 'SubQuery' object
        :param subquery: subquery segment
        :param alias: subquery alias
        :return: 'SubQuery' object
        """
        return SubQuery(subquery, subquery.raw, alias)


class SqlFluffColumn(Column):
    """
    Data Class for SqlFluffColumn
    """

    @staticmethod
    def of(column: BaseSegment, **kwargs) -> Column:
        """
        Build a 'SqlFluffSubQuery' object
        :param column: column segment
        :return:
        """
        if column.type == "select_clause_element":
            source_columns, alias = SqlFluffColumn._get_column_and_alias(column)
            if alias:
                return Column(
                    alias,
                    source_columns=source_columns,
                )
            if source_columns:
                column_name = None
                for sub_segment in list_child_segments(column):
                    if sub_segment.type == "column_reference" or is_wildcard(
                        sub_segment
                    ):
                        if cqt := extract_column_qualifier(sub_segment):
                            column_name = cqt.column
                    elif sub_segment.type == "expression":
                        # special handling for postgres style type cast, col as target column name instead of col::type
                        if len(sub2_segments := list_child_segments(sub_segment)) == 1:
                            if (
                                sub2_segment := sub2_segments[0]
                            ).type == "cast_expression":
                                if (
                                    len(
                                        sub3_segments := list_child_segments(
                                            sub2_segment
                                        )
                                    )
                                    == 2
                                ):
                                    if (
                                        sub3_segment := sub3_segments[0]
                                    ).type == "column_reference":
                                        if cqt := extract_column_qualifier(
                                            sub3_segment
                                        ):
                                            column_name = cqt.column
                return Column(
                    column.raw if column_name is None else column_name,
                    source_columns=source_columns,
                )

        # Wildcard, Case, Function without alias (thus not recognized as an Identifier)
        source_columns = SqlFluffColumn._extract_source_columns(column)
        return Column(
            column.raw,
            source_columns=source_columns,
        )

    @staticmethod
    def _extract_source_columns(segment: BaseSegment) -> List[ColumnQualifierTuple]:
        """
        :param segment: segment to be processed
        :return: list of extracted source columns
        """
        col_list = []
        if segment.type in ("identifier", "column_reference") or is_wildcard(segment):
            if cqt := extract_column_qualifier(segment):
                col_list = [cqt]
        elif segment.type in NON_IDENTIFIER_OR_COLUMN_SEGMENT_TYPE:
            sub_segments = list_child_segments(segment)
            col_list = []
            for sub_segment in sub_segments:
                if sub_segment.type == "bracketed":
                    if is_subquery(sub_segment):
                        col_list += SqlFluffColumn._get_column_from_subquery(
                            sub_segment
                        )
                    else:
                        col_list += SqlFluffColumn._get_column_from_parenthesis(
                            sub_segment
                        )
                elif sub_segment.type in SOURCE_COLUMN_SEGMENT_TYPE or is_wildcard(
                    sub_segment
                ):
                    res = SqlFluffColumn._extract_source_columns(sub_segment)
                    col_list.extend(res)
        return col_list

    @staticmethod
    def _get_column_from_subquery(
        sub_segment: BaseSegment,
    ) -> List[ColumnQualifierTuple]:
        """
        :param sub_segment: segment to be processed
        :return: A list of source columns from a segment
        """
        # This is to avoid circular import
        from sqllineage.runner import LineageRunner

        src_cols = [
            lineage[0]
            for lineage in LineageRunner(
                sub_segment.raw, dialect=SQLPARSE_DIALECT
            ).get_column_lineage(exclude_subquery=False)
        ]
        source_columns = [
            ColumnQualifierTuple(
                src_col.raw_name, src_col.parent.raw_name if src_col.parent else None
            )
            for src_col in src_cols
        ]
        return source_columns

    @staticmethod
    def _get_column_from_parenthesis(
        sub_segment: BaseSegment,
    ) -> List[ColumnQualifierTuple]:
        """
        :param sub_segment: segment to be processed
        :return: list of columns and alias from the segment
        """
        col, _ = SqlFluffColumn._get_column_and_alias(sub_segment)
        if col:
            return col
        col, _ = SqlFluffColumn._get_column_and_alias(sub_segment, False)
        return col if col else []

    @staticmethod
    def _get_column_and_alias(
        segment: BaseSegment, check_bracketed: bool = True
    ) -> Tuple[List[ColumnQualifierTuple], Optional[str]]:
        alias = None
        columns = []
        sub_segments = list_child_segments(segment, check_bracketed)
        for sub_segment in sub_segments:
            if sub_segment.type == "alias_expression":
                alias = extract_identifier(sub_segment)
            elif sub_segment.type in SOURCE_COLUMN_SEGMENT_TYPE or is_wildcard(
                sub_segment
            ):
                res = SqlFluffColumn._extract_source_columns(sub_segment)
                columns += res if res else []
        return columns, alias
