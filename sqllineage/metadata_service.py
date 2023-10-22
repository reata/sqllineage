import abc
from typing import List


class MetaDataService(metaclass=abc.ABCMeta):
    """Abstract class used to provide metadata service like table schema.

    When parse below sql:
    Insert into db1.table1
    select c1
    from db2.table2 t2
    join db3.table3 t3 on t2.id = t3.id

    Only by literal analysis, we don't know which table selected column c1 is from, when parsing column lineage.
    If implement abstract method get_table_columns to provide table schema of table2 and table3,
    then pass the implementation to :class:`sqllineage.runner.LineageRunner`.
    It can help parse column lineage correctly.
    """

    @abc.abstractmethod
    def get_table_columns(self, db: str, table: str, **kwargs) -> List[str]:
        """Get all columns of a table.

        :param db: database name
        :param table: table name
        :return: a list of column names
        """
        raise NotImplementedError
