import warnings

from sqlparse import tokens as T
from sqlparse.sql import (
    Case,
    Comparison,
    Function,
    Identifier,
    Operation,
    Parenthesis,
    Token,
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
    def __init__(self, name: str, schema: Schema = Schema()):
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

    def __str__(self):
        return f"{self.schema}.{self.raw_name.lower()}"

    def __repr__(self):
        return "Table: " + str(self)

    def __eq__(self, other):
        return type(self) is type(other) and str(self) == str(other)

    def __hash__(self):
        return hash(str(self))

    @staticmethod
    def create(identifier: Identifier):
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
        return Table(real_name, schema)


class Partition:
    pass


class Column:
    def __init__(self, name: str, table: Table = None, **kwargs):
        """
        Data Class for Column

        :param name: column name
        :param table: table as defined by :class:`Table`
        :param kwargs:
        """
        if "." not in name:
            if table is None:
                raise SQLLineageException
            else:
                self.table = table
                self.raw_name = escape_identifier_name(name)
        else:
            table_name, column_name = name.rsplit(".", 1)
            self.table = Table(table_name)
            self.raw_name = escape_identifier_name(column_name)
            if table:
                warnings.warn("Name is in table.column format, talbe param is ignored")
        self.source_raw_names = kwargs.pop("source_raw_names", (self.raw_name,))

    def __str__(self):
        return f"{self.table}.{self.raw_name.lower()}"

    def __repr__(self):
        return "Column: " + str(self)

    def __eq__(self, other):
        return type(self) is type(other) and str(self) == str(other)

    def __hash__(self):
        return hash(str(self))

    @staticmethod
    def of(token: Token, table: Table):
        if isinstance(token, Identifier):
            # handle column alias
            alias = token.get_alias()
            if alias:
                kw_idx, kw = token.token_next_by(m=(T.Keyword, "AS"))
                expr = token.token_prev(kw_idx)[1]
                source_raw_names = Column._extract_source_raw_names(expr)
                return Column(alias, table, source_raw_names=source_raw_names)
            else:
                return Column(token.get_real_name(), table)
        else:
            # handle non alias scenario
            source_raw_names = Column._extract_source_raw_names(token)
            return Column(token.value, table, source_raw_names=source_raw_names)

    @staticmethod
    def _extract_source_raw_names(token: Token):
        if isinstance(token, Function):
            # max(col1) AS col2
            source_raw_names = tuple(
                token.get_real_name()
                for token in token.get_parameters()
                if isinstance(token, Identifier)
            )
        elif isinstance(token, (Operation, Parenthesis)):
            # col1 + col2 AS col3
            # (PARTITIONBY col1 ORDERBY col2 DESC)
            source_raw_names = tuple(
                token.get_real_name()
                for token in token.get_sublists()
                if isinstance(token, Identifier)
            )
        elif isinstance(token, Case):
            # CASE WHEN col1 = 2 THEN "V1" WHEN col1 = "2" THEN "V2" END AS col2
            source_raw_names = tuple(
                token.left.get_real_name()
                for token in token.get_sublists()
                if isinstance(token, Comparison)
            )
        else:
            # col1 AS col2
            source_raw_names = (token.value,)
        return source_raw_names

    def to_source_columns(self, table: Table):
        return [
            Column(source_raw_name, table) for source_raw_name in self.source_raw_names
        ]
