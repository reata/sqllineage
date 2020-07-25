import warnings

from sqlparse.sql import Identifier

from sqllineage.exceptions import SQLLineageException


class Schema:
    unknown = "<default>"

    def __init__(self, name: str = unknown):
        self.raw_name = name

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
        if len(name.split(".")) == 2:
            schema_name, table_name = name.split(".")
            self.schema = Schema(schema_name)
            self.raw_name = table_name
            if schema:
                warnings.warn("Name is in schema.table format, schema param is ignored")
        elif "." not in name:
            self.schema = schema
            self.raw_name = name
        else:
            raise SQLLineageException("Invalid format for table name: %s", name)

    def __str__(self):
        return "{}.{}".format(self.schema, self.raw_name.lower())

    def __repr__(self):
        return "Table: " + str(self)

    def __eq__(self, other):
        return type(self) is type(other) and str(self) == str(other)

    def __hash__(self):
        return hash(str(self))

    @staticmethod
    def create(identifier: Identifier):
        schema = (
            Schema(identifier.get_parent_name())
            if identifier.get_parent_name() is not None
            else Schema()
        )
        return Table(identifier.get_real_name(), schema)


class Partition:
    pass


class Column:
    pass
