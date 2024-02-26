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

    def check_schema(schema: str):
        with SQLLineageConfig:
            SQLLineageConfig.DEFAULT_SCHEMA = schema
            return SQLLineageConfig.DEFAULT_SCHEMA

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        executor_dict = {
            executor.submit(check_schema, schema): schema for schema in schema_list
        }
        for executor_work in concurrent.futures.as_completed(executor_dict):
            assert executor_work.result() == executor_dict[executor_work]


def test_config_other():
    with SQLLineageConfig:
        SQLLineageConfig.other = "xxx"
        assert SQLLineageConfig.other == "xxx"
