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
        LineageRunner("with as select * from foo insert into table bar select * from foo")


def test_clause_keyword_after_select():
    with pytest.raises(SQLLineageException):
        LineageRunner("select table as my_column from tab1")


# def test_table_alias_bad():
#     with pytest.raises(KeyError):
#         LineageRunner("select bad.column from tab1")
#     # with pytest.raises(KeyError):
#     #     LineageRunner("select bad.column as col from tab1")
