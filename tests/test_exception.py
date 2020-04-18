import pytest

from sqllineage.core import LineageParser
from sqllineage.exceptions import SQLLineageException


def test_select_without_table():
    with pytest.raises(SQLLineageException):
        LineageParser("select * from where foo='bar'")


def test_insert_without_table():
    with pytest.raises(SQLLineageException):
        LineageParser("insert into select * from foo")


def test_with_cte_without_table():
    with pytest.raises(SQLLineageException):
        LineageParser(
            "with as select * from foo insert into table bar select * from foo"
        )
