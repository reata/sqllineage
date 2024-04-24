import logging
from typing import Any, Dict, List, Optional

from sqlalchemy import MetaData, Table, create_engine, make_url
from sqlalchemy.exc import NoSuchModuleError, NoSuchTableError, OperationalError

from sqllineage.core.metadata_provider import MetaDataProvider
from sqllineage.exceptions import MetaDataProviderException
from sqlalchemy.sql import text

logger = logging.getLogger(__name__)


class SQLAlchemyMetaDataProvider(MetaDataProvider):
    """
    SQLAlchemyMetaDataProvider queries metadata from database using SQLAlchemy
    """

    def __init__(self, url: str, engine_kwargs: Optional[Dict[str, Any]] = None):
        """
        :param url: sqlalchemy url
        :param engine_kwargs: a dictionary of keyword arguments that will be passed to sqlalchemy create_engine
        """
        super().__init__()
        self.metadata_obj = MetaData()
        try:
            if engine_kwargs is None:
                engine_kwargs = {}
            self.engine = create_engine(url, **engine_kwargs)
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
        try:
            sqlalchemy_table = Table(
                table, self.metadata_obj, schema=schema, autoload_with=self.engine
            )
            columns = [c.name for c in sqlalchemy_table.columns]
        except (NoSuchTableError, OperationalError):
            logger.warning(
                "error listing columns for table %s.%s in %s, return empty list instead",
                schema,
                table,
                self.engine.url,
            )
        return columns


class HiveMetaDataProvider(MetaDataProvider):
    """
    HiveMetaDataProvider queries metadata from database using mysql 
    """
    url=""
    def __init__(self, url: str, engine_kwargs: Optional[Dict[str, Any]] = None):
        """
        :param url: sqlalchemy url
        :param engine_kwargs: a dictionary of keyword arguments that will be passed to sqlalchemy create_engine
        """
        super().__init__()
        self.metadata_obj = MetaData()
        try:
            if engine_kwargs is None:
                engine_kwargs = {}
            self.engine = create_engine(url, **engine_kwargs)
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
        try:
            conn=self.engine.connect()
            sql="""select c.COLUMN_NAME from dbs a join tbls b on a.DB_ID =b.DB_ID 
            join sds d on b.SD_ID =d.SD_ID join columns_v2 c on d.CD_ID =c.CD_ID where a.NAME='""" + schema + """' 
            and b.TBL_NAME ='""" + table + """'"""
            result = conn.execute(text(sql)).fetchall()
            columns = [c[0] for c in result]
        except (NoSuchTableError, OperationalError):
            logger.warning(
                "error listing columns for table %s.%s in %s, return empty list instead",
                schema,
                table,
                self.engine.url,
            )
        return columns
