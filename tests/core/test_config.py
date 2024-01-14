import os
from unittest.mock import patch

from sqllineage.config import SQLLineageConfig


def test_config_default():
    assert type(SQLLineageConfig.DIRECTORY) is str
    assert SQLLineageConfig.DEFAULT_SCHEMA is None
    assert type(SQLLineageConfig.TSQL_NO_SEMICOLON) is bool
    assert SQLLineageConfig.TSQL_NO_SEMICOLON is False


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


def test_config_exception():
    is_exception = False
    try:
        SQLLineageConfig.DIRECTORYxxx = "xxx"
    except ValueError:
        is_exception = True
    assert is_exception


def test_config_exception2():
    is_exception = False
    try:
        SQLLineageConfig.DIRECTORY = "xxx"
    except NotADirectoryError:
        is_exception = True
    assert is_exception


def test_config_reset():
    SQLLineageConfig.DIRECTORY = os.path.dirname(__file__)
    assert type(SQLLineageConfig.DIRECTORY) is str
    assert SQLLineageConfig.DIRECTORY == os.path.dirname(__file__)

    SQLLineageConfig.DEFAULT_SCHEMA = "ods"
    assert type(SQLLineageConfig.DEFAULT_SCHEMA) is str
    assert SQLLineageConfig.DEFAULT_SCHEMA == "ods"

    SQLLineageConfig.TSQL_NO_SEMICOLON = True
    assert type(SQLLineageConfig.TSQL_NO_SEMICOLON) is bool
    assert SQLLineageConfig.TSQL_NO_SEMICOLON is True

    SQLLineageConfig.DIRECTORY = None
    SQLLineageConfig.DEFAULT_SCHEMA = None
    SQLLineageConfig.TSQL_NO_SEMICOLON = None
