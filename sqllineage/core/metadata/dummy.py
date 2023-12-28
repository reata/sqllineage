from typing import Dict, List, Optional

from sqllineage.core.metadata_provider import MetaDataProvider


class DummyMetaDataProvider(MetaDataProvider):
    """
    A Dummy MetaDataProvider that accept a dict with table name as key and a set of column name as value
    """

    def __init__(self, metadata: Optional[Dict[str, List[str]]] = None):
        super().__init__()
        self.metadata = metadata if metadata is not None else {}

    def _get_table_columns(self, schema: str, table: str, **kwargs) -> List[str]:
        return self.metadata.get(f"{schema}.{table}", [])

    def __bool__(self):
        return len(self.metadata) > 0
