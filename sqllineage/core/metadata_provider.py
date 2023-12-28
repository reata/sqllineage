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

    def session(self):
        return MetaDataSession(self)

    def __bool__(self):
        """
        bool value tells whether this provider is ready to provide metadata
        """
        return True

    def open(self) -> None:  # noqa
        """Open metadata connection if needed."""
        pass

    def close(self) -> None:
        """Close metadata connection if needed"""
        pass


class MetaDataSession:
    """
    Create an analyzer session which can register session-level metadata as a supplement to global metadata.
    This way, table or views created during the session before available in global metadata can be queried.
    All session-level metadata will be deregistered once session closed.
    """

    def __init__(self, metadata_provider: MetaDataProvider):
        self.metadata_provider = metadata_provider
        self.metadata_provider.open()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.metadata_provider.deregister_session_metadata()
        self.metadata_provider.close()

    def register_session_metadata(
        self, schema: str, table: str, columns: List[str]
    ) -> None:
        self.metadata_provider.register_session_metadata(schema, table, columns)
