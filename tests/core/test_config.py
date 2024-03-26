import concurrent.futures
import os
from multiprocessing import Pool
from unittest.mock import patch

import pytest

from sqllineage.config import SQLLineageConfig
from sqllineage.exceptions import ConfigException


@patch(
    "os.environ",
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


schema_list = ("stg", "ods", "dwd", "dw", "dwa", "dwv")


def check_schema(schema: str):
    with SQLLineageConfig(DEFAULT_SCHEMA=schema):
        # SQLLineageConfig.DEFAULT_SCHEMA = schema
        return SQLLineageConfig.DEFAULT_SCHEMA, schema


def test_config_threading():
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        execute_list = [executor.submit(check_schema, schema) for schema in schema_list]
        for executor_work in concurrent.futures.as_completed(execute_list):
            target, source = executor_work.result()
            assert target == source


def test_config_proecess():
    with Pool(6) as p:
        for work_result in p.imap_unordered(check_schema, schema_list):
            target, source = work_result
            assert target == source
