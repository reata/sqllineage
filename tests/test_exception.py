import pytest

from sqllineage.exceptions import SQLLineageException
from sqllineage.runner import LineageRunner


def test_select_without_table():
    with pytest.raises(SQLLineageException):
        LineageRunner("select * from where foo='bar'")


def test_insert_without_table():
    with pytest.raises(SQLLineageException):
        LineageRunner("insert into select * from foo")


def test_with_cte_without_table():
    with pytest.raises(SQLLineageException):
        LineageRunner(
            "with as select * from foo insert into table bar select * from foo"
        )
