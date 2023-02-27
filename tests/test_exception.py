import pytest

from sqllineage.core.exceptions import SQLLineageException
from sqllineage.runner import LineageRunner


def test_select_without_table():
    with pytest.raises(SQLLineageException):
        LineageRunner("select * from where foo='bar'")._eval()


def test_unparseable_query_in_sqlfluff():
    with pytest.raises(SQLLineageException):
        LineageRunner("WRONG SELECT FROM tab1")._eval()


def test_partial_unparseable_query_in_sqlfluff():
    with pytest.raises(SQLLineageException):
        LineageRunner("SELECT * FROM tab1 AS FULL FULL OUTER JOIN tab2")._eval()
