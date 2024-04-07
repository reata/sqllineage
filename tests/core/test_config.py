import concurrent.futures
import os
import random
import time
from unittest.mock import patch

import pytest

from sqllineage.config import SQLLineageConfig
from sqllineage.exceptions import ConfigException
from sqllineage.runner import LineageRunner


@patch.dict(
    os.environ,
    {
        "SQLLINEAGE_DIRECTORY": os.path.join(os.path.dirname(__file__), "data"),
        "SQLLINEAGE_DEFAULT_SCHEMA": "<default>",
        "SQLLINEAGE_TSQL_NO_SEMICOLON": "true",
    },
)
def test_config():
    assert type(SQLLineageConfig.DIRECTORY) is str
    assert SQLLineageConfig.DIRECTORY == os.path.join(os.path.dirname(__file__), "data")

    assert type(SQLLineageConfig.DEFAULT_SCHEMA) is str
    assert SQLLineageConfig.DEFAULT_SCHEMA == "<default>"

    assert type(SQLLineageConfig.TSQL_NO_SEMICOLON) is bool
    assert SQLLineageConfig.TSQL_NO_SEMICOLON is True


def test_disable_direct_update_config():
    with pytest.raises(ConfigException):
        SQLLineageConfig.DEFAULT_SCHEMA = "ods"


def test_update_config_using_context_manager():
    with SQLLineageConfig(LATERAL_COLUMN_ALIAS_REFERENCE=True):
        assert SQLLineageConfig.LATERAL_COLUMN_ALIAS_REFERENCE is True
    assert SQLLineageConfig.LATERAL_COLUMN_ALIAS_REFERENCE is False

    with SQLLineageConfig(DEFAULT_SCHEMA="ods"):
        assert SQLLineageConfig.DEFAULT_SCHEMA == "ods"
    assert SQLLineageConfig.DEFAULT_SCHEMA == ""

    with SQLLineageConfig(DIRECTORY=""):
        assert SQLLineageConfig.DIRECTORY == ""
    assert SQLLineageConfig.DIRECTORY != ""


def test_update_config_context_manager_non_reentrant():
    with pytest.raises(ConfigException):
        with SQLLineageConfig(DEFAULT_SCHEMA="ods"):
            with SQLLineageConfig(DEFAULT_SCHEMA="dwd"):
                pass


def test_disable_update_unknown_config():
    with pytest.raises(ConfigException):
        with SQLLineageConfig(UNKNOWN_KEY="value"):
            pass


def _check_schema(schema: str):
    # used by test_config_parallel, must be a global function so that it can be pickled between processes
    with SQLLineageConfig(DEFAULT_SCHEMA=schema):
        table = LineageRunner("select * from test").source_tables.pop()
        # randomly sleep [0, 0.1) second to simulate real parsing scenario
        time.sleep(random.random() * 0.1)
        return table.schema.raw_name


@pytest.mark.parametrize("pool", ["ThreadPoolExecutor", "ProcessPoolExecutor"])
def test_config_parallel(pool: str):
    executor_class = getattr(concurrent.futures, pool)
    schemas = [f"db{i}" for i in range(100)]
    with executor_class() as executor:
        futures = [executor.submit(_check_schema, schema) for schema in schemas]
        for i, future in enumerate(futures):
            assert future.result() == schemas[i]
