import os

import pytest

from sqllineage.core.metadata.sqlalchemy import SQLAlchemyMetaDataProvider
from sqllineage.exceptions import MetaDataProviderException


def test_sqlalchemy_metadata_provider_connection_fail():
    # connect to a directory as sqlite db, which is not possible. Simulate connection failure
    with pytest.raises(MetaDataProviderException):
        SQLAlchemyMetaDataProvider(f"sqlite:///{os.path.dirname(__file__)}")


def test_sqlalchemy_metadata_provider_driver_not_install():
    # use an unknown driver to connect. Simulate driver not installed
    with pytest.raises(MetaDataProviderException):
        SQLAlchemyMetaDataProvider("sqlite+unknown_driver:///:memory:")


def test_sqlalchemy_metadata_provider_query_fail():
    provider = SQLAlchemyMetaDataProvider("sqlite:///:memory:")
    assert (
        provider._get_table_columns("non_existing_schema", "non_existing_table") == []
    )
    assert provider._get_table_columns("main", "non_existing_table") == []
