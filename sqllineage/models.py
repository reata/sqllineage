import warnings
from typing import Optional, Set, Union

from sqlparse import tokens as T
from sqlparse.engine import grouping
from sqlparse.sql import (
    Case,
    Comparison,
    Function,
    Identifier,
    Operation,
    Parenthesis,
    Token,
    TokenList,
)
from sqlparse.utils import imt

from sqllineage.exceptions import SQLLineageException
from sqllineage.helpers import escape_identifier_name


class Schema:
    unknown = "<default>"

    def __init__(self, name: str = unknown):
        """
        Data Class for Schema

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


class Table:
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
        self.alias = kwargs.pop("alias", None)

    def __str__(self):
        return f"{self.schema}.{self.raw_name.lower()}"

    def __repr__(self):
        return "Table: " + str(self)

    def __eq__(self, other):
        return type(self) is type(other) and str(self) == str(other)

    def __hash__(self):
        return hash(str(self))

    @staticmethod
    def of(identifier: Identifier):
        # rewrite identifier's get_real_name method, by matching the last dot instead of the first dot, so that the
        # real name for a.b.c will be c instead of b
        dot_idx, _ = identifier._token_matching(
            lambda token: imt(token, m=(T.Punctuation, ".")),
            start=len(identifier.tokens),
            reverse=True,
        )
        real_name = identifier._get_first_name(dot_idx, real_name=True)
        # rewrite identifier's get_parent_name accordingly
        parent_name = (
            "".join(
                [
                    escape_identifier_name(token.value)
                    for token in identifier.tokens[:dot_idx]
                ]
            )
            if dot_idx
            else None
        )
        schema = Schema(parent_name) if parent_name is not None else Schema()
        alias = identifier.get_alias()
        kwargs = {"alias": alias} if alias else {}
        return Table(real_name, schema, **kwargs)


class Partition:
    pass


class SubQuery:
    def __init__(self, token: Parenthesis, query: str, name: Optional[str]):
        """
        Data Class for SubQuery

        :param token: subquery token
        :param query: query text
        :param name: subquery name
        """
        if name is None:
            self.raw_name = "<subquery>"
        else:
            self.raw_name = name
        self.query = query
        self.token = token

    def __str__(self):
        return self.raw_name

    def __repr__(self):
        return "SubQuery: " + str(self)

    @property
    def alias(self):
        return self.raw_name

    @staticmethod
    def of(parenthesis: Parenthesis, name: Optional[str]):
        return SubQuery(parenthesis, parenthesis.value, name)


class Column:
    def __init__(self, name: str, parent: Union[Table, SubQuery] = None, **kwargs):
        """
        Data Class for Column

        :param name: column name
        :param parent: :class:`Table` or :class:`SubQuery`
        :param kwargs:
        """
        if "." not in name:
            if parent is None:
                raise SQLLineageException
            else:
                self.parent = parent
                self.raw_name = escape_identifier_name(name)
        else:
            table_name, column_name = name.rsplit(".", 1)
            self.parent = Table(table_name)
            self.raw_name = escape_identifier_name(column_name)
            if parent:
                warnings.warn(
                    "Name is in parent.column format, parent param is ignored"
                )
        self.source_raw_names = kwargs.pop("source_raw_names", ((self.raw_name, None),))

    def __str__(self):
        return f"{self.parent}.{self.raw_name.lower()}"

    def __repr__(self):
        return "Column: " + str(self)

    def __eq__(self, other):
        return type(self) is type(other) and str(self) == str(other)

    def __hash__(self):
        return hash(str(self))

    @staticmethod
    def of(token: Token, parent: Union[Table, SubQuery]):
        if isinstance(token, Identifier):
            alias = token.get_alias()
            if alias:
                # handle column alias, including alias for column name or Case, Function
                kw_idx, kw = token.token_next_by(m=(T.Keyword, "AS"))
                idx, _ = token.token_prev(kw_idx, skip_cm=True)
                expr = grouping.group(TokenList(token.tokens[: idx + 1]))[0]
                source_raw_names = Column._extract_source_raw_names(expr)
                return Column(alias, parent, source_raw_names=source_raw_names)
            else:
                # select column name directly without alias
                return Column(
                    token.get_real_name(),
                    parent,
                    source_raw_names=(
                        (token.get_real_name(), token.get_parent_name()),
                    ),
                )
        else:
            # Wildcard, Case, Function without alias (thus not recognized as an Identifier)
            source_raw_names = Column._extract_source_raw_names(token)
            return Column(token.value, parent, source_raw_names=source_raw_names)

    @staticmethod
    def _extract_source_raw_names(token: Token):
        if isinstance(token, Function):
            # max(col1) AS col2
            source_raw_names = tuple(
                (token.get_real_name(), token.get_parent_name())
                for token in token.get_parameters()
                if isinstance(token, Identifier)
            )
        elif isinstance(token, (Operation, Parenthesis)):
            # col1 + col2 AS col3
            # (PARTITIONBY col1 ORDERBY col2 DESC)
            source_raw_names = tuple(
                (token.get_real_name(), token.get_parent_name())
                for token in token.get_sublists()
                if isinstance(token, Identifier)
            )
        elif isinstance(token, Case):
            # CASE WHEN col1 = 2 THEN "V1" WHEN col1 = "2" THEN "V2" END AS col2
            source_raw_names = tuple(
                (token.left.get_real_name(), token.get_parent_name())
                for token in token.get_sublists()
                if isinstance(token, Comparison)
            )
        elif isinstance(token, Identifier):
            # col1 AS col2
            source_raw_names = ((token.get_real_name(), token.get_parent_name()),)
        else:
            # select *
            source_raw_names = ((token.value, None),)
        return source_raw_names

    def to_source_columns(self, source_tables: Set[Union[SubQuery, Table]]):
        table_dict = {table.raw_name: table for table in source_tables}
        table_dict.update(
            {table.alias: table for table in source_tables if table.alias}
        )
        source_columns = []
        for (src_col, src_tbl) in self.source_raw_names:
            if src_tbl is None:
                # SELECT * OR SELECT col (without table prefix), src_tbl is NONE
                for table in source_tables:
                    # TODO: add some mark so that we know column might not exist in this table
                    source_columns.append(Column(src_col, table))
            else:
                # any chance we might see dict.get return None here?
                source_columns.append(Column(src_col, table_dict.get(src_tbl)))
        return source_columns
