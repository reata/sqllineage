import concurrent.futures
import os
from unittest.mock import patch

from sqllineage.config import SQLLineageConfig


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


def test_config_threading():
    schema_list = ("stg", "ods", "dwd", "dw", "dwa", "dwv")

    def check_schema(default_schema: str):
        with SQLLineageConfig:
            SQLLineageConfig["DEFAULT_SCHEMA"] = default_schema
            assert SQLLineageConfig.DEFAULT_SCHEMA == default_schema

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        for default_schema in schema_list:
            executor.submit(check_schema, default_schema)
