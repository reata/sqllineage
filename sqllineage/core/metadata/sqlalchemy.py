import logging
from typing import List

from sqlalchemy import MetaData, Table, create_engine, inspect, make_url
from sqlalchemy.exc import NoSuchModuleError, NoSuchTableError, OperationalError

from sqllineage.core.metadata_provider import MetaDataProvider
from sqllineage.exceptions import MetaDataProviderException


logger = logging.getLogger(__name__)


class SQLAlchemyMetaDataProvider(MetaDataProvider):
    """
    SQLAlchemyMetaDataProvider queries metadata from database using SQLAlchemy
    """

    def __init__(self, url: str):
        super().__init__()
        self.metadata_obj = MetaData()
        try:
            self.engine = create_engine(url)
        except NoSuchModuleError as e:
            u = make_url(url)
            raise MetaDataProviderException(
                f"SQLAlchemy dialect driver {u.drivername} is not installed correctly"
            ) from e
        try:
            self.engine.connect()
        except OperationalError as e:
            raise MetaDataProviderException(f"Could not connect to {url}") from e

    def _get_table_columns(self, schema: str, table: str, **kwargs) -> List[str]:
        columns = []
        if inspect(self.engine).has_schema(schema):
            try:
                sqlalchemy_table = Table(
                    table, self.metadata_obj, schema=schema, autoload_with=self.engine
                )
                columns = [c.name for c in sqlalchemy_table.columns]
            except NoSuchTableError:
                logger.warning("%s does not exist in database %s", table, schema)
        else:
            logger.warning("Schema %s does not exist", schema)
        return columns
