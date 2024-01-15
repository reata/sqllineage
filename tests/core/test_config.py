import os
from pathlib import Path
from unittest.mock import patch

import pytest

from sqllineage.config import SQLLineageConfig


def test_config_default_value():
    assert (
        SQLLineageConfig.DIRECTORY
        == Path(os.path.dirname(__file__))
        .parent.parent.joinpath("sqllineage", "data")
        .__str__()
    )
    assert SQLLineageConfig.DEFAULT_SCHEMA is None
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


@patch(
    "os.environ",
    {
        "SQLLINEAGE_DIRECTORY": os.path.join(os.path.dirname(__file__), "data"),
        "SQLLINEAGE_DEFAULT_SCHEMA": "<default>",
        "SQLLINEAGE_TSQL_NO_SEMICOLON": "true",
    },
)
def test_config_reset():
    SQLLineageConfig.DIRECTORY = os.path.join(os.path.dirname(__file__), "")
    assert type(SQLLineageConfig.DIRECTORY) is str
    assert SQLLineageConfig.DIRECTORY == os.path.join(os.path.dirname(__file__), "")

    SQLLineageConfig.DEFAULT_SCHEMA = "ods"
    assert type(SQLLineageConfig.DEFAULT_SCHEMA) is str
    assert SQLLineageConfig.DEFAULT_SCHEMA == "ods"

    SQLLineageConfig.TSQL_NO_SEMICOLON = True
    assert type(SQLLineageConfig.TSQL_NO_SEMICOLON) is bool
    assert SQLLineageConfig.TSQL_NO_SEMICOLON is True

    SQLLineageConfig.DIRECTORY = None
    SQLLineageConfig.DEFAULT_SCHEMA = None
    SQLLineageConfig.TSQL_NO_SEMICOLON = None


def test_config_exception():
    with pytest.raises(ValueError):
        SQLLineageConfig.DIRECTORYxx = "xx"

    with pytest.raises(ValueError):
        SQLLineageConfig.DEFAULT_SCHEMA = False
