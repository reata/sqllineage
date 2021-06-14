import warnings

from sqlparse.sql import Identifier
from sqlparse.tokens import Punctuation
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
            lambda token: imt(token, m=(Punctuation, ".")),
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
    pass
