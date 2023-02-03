import pytest

from sqllineage.exceptions import SQLLineageException
from sqllineage.runner import LineageRunner


def test_select_without_table():
    with pytest.raises(SQLLineageException):
        LineageRunner("select * from where foo='bar'")._eval()


def test_unparseable_query_in_sqlfluff():
    with pytest.raises(SQLLineageException):
        LineageRunner("WRONG SELECT FROM tab1", use_sqlfluff=True)._eval()
