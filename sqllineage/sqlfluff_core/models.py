import warnings
from typing import Optional, Tuple, Callable, NamedTuple

from sqlfluff.core.parser import BaseSegment

from sqllineage.core.models import SubQuery, Schema, Table, Column
from sqllineage.exceptions import SQLLineageException
from typing import List, Union, Dict, Set
from sqllineage.utils.entities import ColumnQualifierTuple
from sqllineage.utils.helpers import escape_identifier_name
from sqllineage.sqlfluff_core.utils.sqlfluff import (
    retrieve_segments,
    is_wildcard,
    is_subquery,
)

SOURCE_COLUMN_SEGMENT_TYPE = [
    "identifier",
    "column_reference",
    "function",
    "over_clause",
    "over_clause",
    "partitionby_clause",
    "orderby_clause",
    "expression",
    "case_expression",
    "when_clause",
    "else_clause",
]


class SqlFluffTable(Table):
    def __init__(self, name: str, schema: Schema = Schema(), **kwargs):
        """
        Data Class for Table

        :param name: table name
        :param schema: schema as defined by :class:`Schema`
        """
        if "." not in name:
            self.schema = schema
            self.raw_name = escape_identifier_name(name)
        else:
            schema_name, table_name = name.rsplit(".", 1)
            if len(schema_name.split(".")) > 2:
                # allow db.schema as schema_name, but a.b.c as schema_name is forbidden
                raise SQLLineageException("Invalid format for table name: %s.", name)
            self.schema = Schema(schema_name)
            self.raw_name = escape_identifier_name(table_name)
            if schema:
                warnings.warn("Name is in schema.table format, schema param is ignored")
        self.alias = kwargs.pop("alias", self.raw_name)

    @staticmethod
    def of(segment: BaseSegment, alias: str = None) -> "Table":
        # rewrite identifier's get_real_name method, by matching the last dot instead of the first dot, so that the
        # real name for a.b.c will be c instead of b
        dot_idx, _ = _token_matching(
            segment,
            lambda s: s.type == "symbol",
            start=len(segment.segments),
            reverse=True,
        )
        real_name = (
            segment.segments[dot_idx + 1].raw if dot_idx else segment.segments[0].raw
        )
        # rewrite identifier's get_parent_name accordingly
        parent_name = (
            "".join(
                [
                    escape_identifier_name(segment.raw)
                    for segment in segment.segments[:dot_idx]
                ]
            )
            if dot_idx
            else None
        )
        schema = Schema(parent_name) if parent_name is not None else Schema()
        kwargs = {"alias": alias} if alias else {}
        return SqlFluffTable(real_name, schema, **kwargs)


class SqlFluffSubQuery(SubQuery):
    def __init__(self, bracketed_segment: BaseSegment, alias: Optional[str]):
        """
        Data Class for SqlFluffSubQuery

        :param bracketed_segment: subquery segment
        :param alias: subquery alias
        """
        self.segment = bracketed_segment
        self._query = bracketed_segment.raw
        self.alias = alias if alias is not None else f"subquery_{hash(self)}"

    @staticmethod
    def of(bracketed_segment: BaseSegment, alias: Optional[str]) -> "SqlFluffSubQuery":
        return SqlFluffSubQuery(bracketed_segment, alias)


class SqlFluffColumn(Column):
    def __init__(self, name: str, **kwargs):
        """
        Data Class for Column

        :param name: column name
        :param parent: :class:`Table` or :class:`SubQuery`
        :param kwargs:
        """
        self._parent: Set[Union[Table, SubQuery]] = set()
        self.raw_name = escape_identifier_name(name)
        self.source_columns = kwargs.pop("source_columns", ((self.raw_name, None),))

    @staticmethod
    def of(segment: BaseSegment):
        if segment.type == "select_clause_element":
            source_columns, alias = SqlFluffColumn._get_column_and_alias(segment)
            if alias:
                return SqlFluffColumn(
                    alias,
                    source_columns=source_columns,
                )
            if source_columns:
                sub_segments = retrieve_segments(segment)
                column_name = None
                for sub_segment in sub_segments:
                    if sub_segment.type == "column_reference":
                        column_name = SqlFluffColumn._get_identifier(sub_segment)

                return SqlFluffColumn(
                    segment.raw if column_name is None else column_name,
                    source_columns=source_columns,
                )

        # Wildcard, Case, Function without alias (thus not recognized as an Identifier)
        source_columns = SqlFluffColumn._extract_source_columns(segment)
        return SqlFluffColumn(
            segment.raw,
            source_columns=source_columns,
        )

    @staticmethod
    def _extract_source_columns(segment: BaseSegment) -> List[ColumnQualifierTuple]:
        if segment.type == "identifier" or is_wildcard(segment):
            return [ColumnQualifierTuple(segment.raw, None)]
        if segment.type == "column_reference":
            column = SqlFluffColumn._get_identifier(segment)
            return [ColumnQualifierTuple(column, None)]
        if segment.type in [
            "function",
            "over_clause",
            "partitionby_clause",
            "orderby_clause",
            "expression",
            "case_expression",
            "when_clause",
            "else_clause",
            "select_clause_element",
        ]:
            sub_segments = retrieve_segments(segment)
            col_list = []
            for sub_segment in sub_segments:
                if sub_segment.type == "bracketed":
                    if is_subquery(sub_segment):
                        col_list += SqlFluffColumn.get_column_from_subquery(sub_segment)
                    else:
                        col_list += SqlFluffColumn.get_column_from_parenthesis(
                            sub_segment
                        )
                elif sub_segment.type in SOURCE_COLUMN_SEGMENT_TYPE or is_wildcard(
                    sub_segment
                ):
                    res = SqlFluffColumn._extract_source_columns(sub_segment)
                    col_list.extend(res)
            return col_list

    @staticmethod
    def get_column_from_subquery(
        sub_segment: BaseSegment,
    ) -> List[ColumnQualifierTuple]:
        # This is to avoid circular import
        from sqllineage.runner import LineageRunner

        src_cols = [
            lineage[0]
            for lineage in LineageRunner(sub_segment.raw).get_column_lineage(
                exclude_subquery=False
            )
        ]
        source_columns = [
            ColumnQualifierTuple(src_col.raw_name, src_col.parent.raw_name)
            for src_col in src_cols
        ]
        return source_columns

    @staticmethod
    def get_column_from_parenthesis(
        sub_segment: BaseSegment,
    ) -> List[ColumnQualifierTuple]:
        col, _ = SqlFluffColumn._get_column_and_alias(sub_segment)
        if col:
            return col
        col, _ = SqlFluffColumn._get_column_and_alias(sub_segment, False)
        return col if col else []

    @staticmethod
    def _get_column_and_alias(
        segment: BaseSegment, check_bracketed: bool = True
    ) -> Tuple[List[ColumnQualifierTuple], str]:
        alias = None
        column = []
        sub_segments = retrieve_segments(segment, check_bracketed)
        for sub_segment in sub_segments:
            if sub_segment.type == "alias_expression":
                alias = SqlFluffColumn._get_identifier(sub_segment)
            elif sub_segment.type in SOURCE_COLUMN_SEGMENT_TYPE or is_wildcard(
                sub_segment
            ):
                res = SqlFluffColumn._extract_source_columns(sub_segment)
                column += res if res else []

        return column, alias

    @staticmethod
    def _add_to_col_list(column: Union[str, List[str]], column_list: List[str]) -> None:
        if column:
            if isinstance(column, list):
                column_list.extend(column)
            else:
                column_list.append((column, None))

    @staticmethod
    def _get_identifier(col_segment: BaseSegment) -> str:
        identifiers = retrieve_segments(col_segment)
        col_identifier = identifiers[-1]
        return col_identifier.raw

    def to_source_columns(self, alias_mapping: Dict[str, Union[Table, SubQuery]]):
        """
        Best guess for source table given all the possible table/subquery and their alias.
        """

        def _to_src_col(name: str, parent: Union[Table, SubQuery] = None):
            col = SqlFluffColumn(name)
            if parent:
                col.parent = parent
            return col

        source_columns = set()
        for (src_col, qualifier) in self.source_columns:
            if qualifier is None:
                if src_col == "*":
                    # select *
                    for table in set(alias_mapping.values()):
                        source_columns.add(_to_src_col(src_col, table))
                else:
                    # select unqualified column
                    src_col = _to_src_col(src_col, None)
                    for table in set(alias_mapping.values()):
                        # in case of only one table, we get the right answer
                        # in case of multiple tables, a bunch of possible tables are set
                        src_col.parent = table
                    source_columns.add(src_col)
            else:
                if alias_mapping.get(qualifier):
                    source_columns.add(
                        _to_src_col(src_col, alias_mapping.get(qualifier))
                    )
                else:
                    source_columns.add(_to_src_col(src_col, Table(qualifier)))
        return source_columns


class SqlFluffAnalyzerContext(NamedTuple):
    subquery: Optional[SqlFluffSubQuery] = None
    prev_cte: Optional[Set[SqlFluffSubQuery]] = None
    prev_write: Optional[Set[SqlFluffSubQuery]] = None


def _token_matching(
    segment: BaseSegment,
    funcs: Callable,
    start: int = 0,
    end: int = None,
    reverse: bool = False,
):
    """next token that match functions"""
    if start is None:
        return None

    if not isinstance(funcs, (list, tuple)):
        funcs = (funcs,)

    if reverse:
        assert end is None
        indexes = range(start - 2, -1, -1)
    else:
        if end is None:
            end = len(segment.segments)
        indexes = range(start, end)
    for idx in indexes:
        token = segment.segments[idx]
        for func in funcs:
            if func(token):
                return idx, token
    return None, None
