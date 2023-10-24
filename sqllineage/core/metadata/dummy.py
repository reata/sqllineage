from typing import Dict, List

from sqllineage.core.metadata_provider import MetaDataProvider


class DummyMetaDataProvider(MetaDataProvider):
    """
    A Dummy MetaDataProvider that accept a dict with table name as key and a set of column name as value
    """

    def __init__(self, schemas: Dict[str, List[str]]):
        self.schemas = schemas

    def get_table_columns(self, db: str, table: str, **kwargs) -> List[str]:
        return self.schemas.get(f"{db}.{table}", [])
