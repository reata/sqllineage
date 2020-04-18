from typing import Optional


class Database:
    def __init__(self, name: Optional[str] = "<unknown>"):
        self.raw_name = name

    def __str__(self):
        return self.raw_name.lower()

    def __repr__(self):
        return "Database: " + str(self)

    def __eq__(self, other):
        return type(self) is type(other) and str(self) == str(other)

    def __hash__(self):
        return hash(str(self))


class Table:
    def __init__(self, name: str, database: Optional[Database] = Database()):
        self.database = database
        self.raw_name = name

    def __str__(self):
        return "{}.{}".format(self.database, self.raw_name.lower())

    def __repr__(self):
        return "Table: " + str(self)

    def __eq__(self, other):
        return type(self) is type(other) and str(self) == str(other)

    def __hash__(self):
        return hash(str(self))


class Partition:
    pass


class Column:
    pass
