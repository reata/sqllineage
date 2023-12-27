from abc import abstractmethod
from typing import Dict, List


class MetaDataProvider:
    """Base class used to provide metadata service like table schema.

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

    def __init__(self) -> None:
        self._session_metadata: Dict[str, List[str]] = {}

    def get_table_columns(self, schema: str, table: str, **kwargs) -> List[str]:
        key = f"{schema}.{table}"
        if key in self._session_metadata:
            return self._session_metadata[key]
        else:
            return self._get_table_columns(schema, table, **kwargs)

    @abstractmethod
    def _get_table_columns(self, schema: str, table: str, **kwargs) -> List[str]:
        """To be implemented by subclasses."""

    def register_session_metadata(
        self, schema: str, table: str, columns: List[str]
    ) -> None:
        """Register session-level metadata, like temporary table or view created."""
        self._session_metadata[f"{schema}.{table}"] = columns

    def deregister_session_metadata(self) -> None:
        """Deregister session-level metadata."""
        self._session_metadata.clear()

    def __bool__(self):
        """
        bool value tells whether this provider is ready to provide metadata
        """
        return True
