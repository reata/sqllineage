import warnings
from typing import Dict, List, Set, Union
from typing import Optional, Tuple

from sqlfluff.core.parser import BaseSegment

from sqllineage.exceptions import SQLLineageException
from sqllineage.sqlfluff_core.utils.entities import SqlFluffColumnQualifierTuple
from sqllineage.sqlfluff_core.utils.sqlfluff import (
    get_identifier,
    is_subquery,
    is_wildcard,
    retrieve_segments,
    token_matching,
)
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
]

SOURCE_COLUMN_SEGMENT_TYPE = NON_IDENTIFIER_OR_COLUMN_SEGMENT_TYPE + [
    "identifier",
    "column_reference",
]


class SqlFluffSchema:
    """
    Data Class for Schema
    """

    unknown = "<default>"

    def __init__(self, name: str = unknown):
        """
        :param name: schema name
        """
        self.raw_name = escape_identifier_name(name)

    def __str__(self):
        return self.raw_name.lower()

    def __repr__(self):
        return "Schema: " + str(self)

    def __eq__(self, other):
        return type(self) is type(other) and str(self) == str(other)

    def __hash__(self):
        return hash(str(self))

    def __bool__(self):
        return str(self) != self.unknown


class SqlFluffPath:
    """
    Data Class for SqlFluffPath
    """

    def __init__(self, uri: str):
        """
        :param uri: uri of the path
        """
        self.uri = escape_identifier_name(uri)

    def __str__(self):
        return self.uri

    def __repr__(self):
        return "Path: " + str(self)

    def __eq__(self, other):
        return type(self) is type(other) and self.uri == other.uri

    def __hash__(self):
        return hash(self.uri)


class SqlFluffTable:
    """
    Data Class for SqlFluffTable
    """

    def __init__(self, name: str, schema: SqlFluffSchema = SqlFluffSchema(), **kwargs):
        """
        :param name: table name
        :param schema: schema as defined by 'SqlFluffTable'
        """
        if "." not in name:
            self.schema = schema
            self.raw_name = escape_identifier_name(name)
        else:
            schema_name, table_name = name.rsplit(".", 1)
            if len(schema_name.split(".")) > 2:
                # allow db.schema as schema_name, but a.b.c as schema_name is forbidden
                raise SQLLineageException("Invalid format for table name: %s.", name)
            self.schema = SqlFluffSchema(schema_name)
            self.raw_name = escape_identifier_name(table_name)
            if schema:
                warnings.warn("Name is in schema.table format, schema param is ignored")
        self.alias = kwargs.pop("alias", self.raw_name)

    def __str__(self):
        return f"{self.schema}.{self.raw_name.lower()}"

    def __repr__(self):
        return "Table: " + str(self)

    def __eq__(self, other):
        return type(self) is type(other) and str(self) == str(other)

    def __hash__(self):
        return hash(str(self))

    @staticmethod
    def of(table_segment: BaseSegment, alias: Optional[str] = None) -> "SqlFluffTable":
        """
        Build an object of type 'SqlFluffTable'
        :param table_segment: table segment to be processed
        :param alias: alias of the table segment
        :return: 'SqlFluffTable' object
        """
        # rewrite identifier's get_real_name method, by matching the last dot instead of the first dot, so that the
        # real name for a.b.c will be c instead of b
        dot_idx, _ = token_matching(
            table_segment,
            (lambda s: bool(s.type == "symbol"),),
            start=len(table_segment.segments),
            reverse=True,
        )
        real_name = (
            table_segment.segments[dot_idx + 1].raw
            if dot_idx
            else (
                table_segment.raw
                if table_segment.type == "identifier"
                else table_segment.segments[0].raw
            )
        )
        # rewrite identifier's get_parent_name accordingly
        parent_name = (
            "".join(
                [
                    escape_identifier_name(segment.raw)
                    for segment in table_segment.segments[:dot_idx]
                ]
            )
            if dot_idx
            else None
        )
        schema = (
            SqlFluffSchema(parent_name) if parent_name is not None else SqlFluffSchema()
        )
        kwargs = {"alias": alias} if alias else {}
        return SqlFluffTable(real_name, schema, **kwargs)


class SqlFluffSubQuery:
    """
    Data Class for SqlFluffSubQuery
    """

    def __init__(self, subquery: BaseSegment, alias: Optional[str]):
        """
        :param subquery: subquery segment
        :param alias: subquery alias
        """
        self.segment = subquery
        self._query = subquery.raw
        self.alias = alias if alias is not None else f"subquery_{hash(self)}"

    def __str__(self):
        return self.alias

    def __repr__(self):
        return "SubQuery: " + str(self)

    def __eq__(self, other):
        return type(self) is type(other) and self._query == other._query

    def __hash__(self):
        return hash(self._query)

    @staticmethod
    def of(subquery: BaseSegment, alias: Optional[str]) -> "SqlFluffSubQuery":
        """
        Build a 'SqlFluffSubQuery' object
        :param subquery: subquery segment
        :param alias: subquery alias
        :return: 'SqlFluffSubQuery' object
        """
        return SqlFluffSubQuery(subquery, alias)


class SqlFluffColumn:
    """
    Data Class for SqlFluffColumn
    """

    def __init__(self, name: str, **kwargs):
        """
        :param name: column name
        :param parent: 'SqlFluffSubQuery' or 'SqlFluffTable' object
        :param kwargs:
        """
        self._parent: Set[Union[SqlFluffTable, SqlFluffSubQuery]] = set()
        self.raw_name = escape_identifier_name(name)
        self.source_columns = kwargs.pop("source_columns", ((self.raw_name, None),))

    def __str__(self):
        return (
            f"{self.parent}.{self.raw_name.lower()}"
            if self.parent is not None and not isinstance(self.parent, SqlFluffPath)
            else f"{self.raw_name.lower()}"
        )

    def __repr__(self):
        return "Column: " + str(self)

    def __eq__(self, other):
        return type(self) is type(other) and str(self) == str(other)

    def __hash__(self):
        return hash(str(self))

    @property
    def parent(self) -> Optional[Union[SqlFluffTable, SqlFluffSubQuery]]:
        """
        :return: parent of the table
        """
        return list(self._parent)[0] if len(self._parent) == 1 else None

    @parent.setter
    def parent(self, value: Union[SqlFluffTable, SqlFluffSubQuery]):
        self._parent.add(value)

    @property
    def parent_candidates(self) -> List[Union[SqlFluffTable, SqlFluffSubQuery]]:
        """
        :return: parent candidate list
        """
        return sorted(self._parent, key=lambda p: str(p))

    @staticmethod
    def of(column_segment: BaseSegment, dialect: str) -> "SqlFluffColumn":
        """
        Build a 'SqlFluffSubQuery' object
        :param column_segment: column segment
        :param dialect: dialect to be used in case of running the 'LineageRunner'
        :return:
        """
        if column_segment.type == "select_clause_element":
            source_columns, alias = SqlFluffColumn._get_column_and_alias(
                column_segment, dialect
            )
            if alias:
                return SqlFluffColumn(
                    alias,
                    source_columns=source_columns,
                )
            if source_columns:
                sub_segments = retrieve_segments(column_segment)
                column_name = None
                for sub_segment in sub_segments:
                    if sub_segment.type == "column_reference":
                        column_name = get_identifier(sub_segment)

                return SqlFluffColumn(
                    column_segment.raw if column_name is None else column_name,
                    source_columns=source_columns,
                )

        # Wildcard, Case, Function without alias (thus not recognized as an Identifier)
        source_columns = SqlFluffColumn._extract_source_columns(column_segment, dialect)
        return SqlFluffColumn(
            column_segment.raw,
            source_columns=source_columns,
        )

    @staticmethod
    def _extract_source_columns(
        segment: BaseSegment, dialect: str
    ) -> List[SqlFluffColumnQualifierTuple]:
        """

        :param segment:
        :param dialect:
        :return:
        """
        if segment.type == "identifier" or is_wildcard(segment):
            return [SqlFluffColumnQualifierTuple(segment.raw, None)]
        if segment.type == "column_reference":
            parent, column = SqlFluffColumn._get_column_and_parent(segment)
            return [SqlFluffColumnQualifierTuple(column, parent)]
        if segment.type in NON_IDENTIFIER_OR_COLUMN_SEGMENT_TYPE:
            sub_segments = retrieve_segments(segment)
            col_list = []
            for sub_segment in sub_segments:
                if sub_segment.type == "bracketed":
                    if is_subquery(sub_segment):
                        col_list += SqlFluffColumn._get_column_from_subquery(
                            sub_segment, dialect
                        )
                    else:
                        col_list += SqlFluffColumn._get_column_from_parenthesis(
                            sub_segment, dialect
                        )
                elif sub_segment.type in SOURCE_COLUMN_SEGMENT_TYPE or is_wildcard(
                    sub_segment
                ):
                    res = SqlFluffColumn._extract_source_columns(sub_segment, dialect)
                    col_list.extend(res)
            return col_list
        return []

    @staticmethod
    def _get_column_from_subquery(
        sub_segment: BaseSegment, dialect: str
    ) -> List[SqlFluffColumnQualifierTuple]:
        """

        :param sub_segment:
        :param dialect:
        :return:
        """
        # This is to avoid circular import
        from sqllineage.runner import LineageRunner

        src_cols = [
            lineage[0]
            for lineage in LineageRunner(
                sub_segment.raw, dialect=dialect, use_sqlparse=False
            ).get_column_lineage(exclude_subquery=False)
        ]
        source_columns = [
            SqlFluffColumnQualifierTuple(src_col.raw_name, src_col.parent.raw_name)
            for src_col in src_cols
        ]
        return source_columns

    @staticmethod
    def _get_column_from_parenthesis(
        sub_segment: BaseSegment,
        dialect: str,
    ) -> List[SqlFluffColumnQualifierTuple]:
        """

        :param sub_segment:
        :param dialect:
        :return:
        """
        col, _ = SqlFluffColumn._get_column_and_alias(sub_segment, dialect)
        if col:
            return col
        col, _ = SqlFluffColumn._get_column_and_alias(sub_segment, dialect, False)
        return col if col else []

    @staticmethod
    def _get_column_and_alias(
        segment: BaseSegment, dialect: str, check_bracketed: bool = True
    ) -> Tuple[List[SqlFluffColumnQualifierTuple], Optional[str]]:
        alias = None
        columns = []
        sub_segments = retrieve_segments(segment, check_bracketed)
        for sub_segment in sub_segments:
            if sub_segment.type == "alias_expression":
                alias = get_identifier(sub_segment)
            elif sub_segment.type in SOURCE_COLUMN_SEGMENT_TYPE or is_wildcard(
                sub_segment
            ):
                res = SqlFluffColumn._extract_source_columns(sub_segment, dialect)
                columns += res if res else []

        return columns, alias

    @staticmethod
    def _get_column_and_parent(col_segment: BaseSegment) -> Tuple[Optional[str], str]:
        identifiers = retrieve_segments(col_segment)
        if len(identifiers) > 1:
            return identifiers[-2].raw, identifiers[-1].raw
        return None, identifiers[-1].raw

    def to_source_columns(
        self, alias_mapping: Dict[str, Union[SqlFluffTable, SqlFluffSubQuery]]
    ):
        """
        Best guess for source table given all the possible table/subquery and their alias.
        """

        def _to_src_col(
            name: str, parent: Optional[Union[SqlFluffTable, SqlFluffSubQuery]] = None
        ):
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
                    source_columns.add(_to_src_col(src_col, SqlFluffTable(qualifier)))
        return source_columns


class SqlFluffAnalyzerContext:
    """
    Data class to hold the analyzer context
    """

    subquery: Optional[SqlFluffSubQuery]
    prev_cte: Optional[Set[SqlFluffSubQuery]]
    prev_write: Optional[Set[Union[SqlFluffSubQuery, SqlFluffTable]]]

    def __init__(
        self,
        subquery: Optional[SqlFluffSubQuery] = None,
        prev_cte: Optional[Set[SqlFluffSubQuery]] = None,
        prev_write: Optional[Set[Union[SqlFluffSubQuery, SqlFluffTable]]] = None,
    ):
        self.subquery = subquery
        self.prev_cte = prev_cte
        self.prev_write = prev_write
