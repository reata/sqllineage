import warnings

from sqlparse.sql import Identifier

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
    def __init__(self, name: str, schema: Schema = Schema(), alias: str = None):
        """
        Data Class for Table

        :param name: table name
        :param schema: schema as defined by :class:`Schema`
        """
        self.alias = alias

        if len(name.split(".")) == 2:
            schema_name, table_name = name.split(".")
            self.schema = Schema(schema_name)
            self.raw_name = escape_identifier_name(table_name)
            if schema:
                warnings.warn("Name is in schema.table format, schema param is ignored")
        elif "." not in name:
            self.schema = schema
            self.raw_name = escape_identifier_name(name)
        else:
            raise SQLLineageException("Invalid format for table name: %s", name)

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
        schema = (
            Schema(identifier.get_parent_name())
            if identifier.get_parent_name() is not None
            else Schema()
        )
        return Table(identifier.get_real_name(), schema, identifier.get_alias())


class Partition:
    pass


class Column:
    def __init__(self, name: str, parent: Table, alias: str = None):
        """
        Data Class for Schema

        :param name: schema name
        """
        self.name = escape_identifier_name(name)
        self.parent = parent
        self.alias = escape_identifier_name(alias) if alias else None

    def __str__(self):
        return self.name.lower() if not self.parent else str(self.parent) + "." + self.name.lower()

    def __repr__(self):
        if not self.alias:
            return "Column: " + str(self)
        else:
            return "Aliased column: " + str(self) + " as " + self.alias.lower()

    def __eq__(self, other):
        return type(self) is type(other) and str(self) == str(other)

    def __hash__(self):
        return hash(str(self))

    # def __bool__(self):
    #     return str(self) != self.unknown

    # @staticmethod
    # def create(identifier: Identifier):
    #     return Column(
    #         identifier.get_real_name(), identifier.get_alias(), identifier.get_parent_name()
    #     )


# class DataSet:
#     def __init__(self, name: str, schema: Schema = Schema()):
#         class_name = self.__class__.__name__.lower()
#         if len(name.split(".")) == 2:
#             schema_name, table_name = name.split(".")
#             self.schema = Schema(schema_name)
#             self.raw_name = table_name
#             if schema:
#                 warnings.warn("Name is in schema.{} format, schema param is ignored".format(class_name))
#         elif "." not in name:
#             self.schema = schema
#             self.raw_name = name
#         else:
#             raise SQLLineageException("Invalid format for {} name: {}".format(class_name, name))
#
#     def __str__(self):
#         return "{}.{}".format(self.schema, self.raw_name.lower())
#
#     def __repr__(self):
#         return self.__class__.__name__ + ": " + str(self)
#
#     def __eq__(self, other):
#         return type(self) is type(other) and str(self) == str(other)
#
#     def __hash__(self):
#         return hash(str(self))
#
#     @staticmethod
#     def create(identifier: Identifier, is_view=False):
#         schema = (
#             Schema(identifier.get_parent_name())
#             if identifier.get_parent_name() is not None
#             else Schema()
#         )
#         clazz = View if is_view else Table
#         return clazz(identifier.get_real_name(), schema)
#
#
# class Table(DataSet):
#     pass
#
#
# class View(DataSet):
#     pass
