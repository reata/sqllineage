import pytest

from sqllineage.exceptions import (
    InvalidSyntaxException,
    SQLLineageException,
    UnsupportedStatementException,
)
from sqllineage.runner import LineageRunner


def test_select_without_table():
    with pytest.raises(SQLLineageException):
        LineageRunner("select * from where foo='bar'")._eval()


def test_unsupported_query_type_in_sqlfluff():
    with pytest.raises(UnsupportedStatementException):
        LineageRunner("WRONG SELECT FROM tab1")._eval()


def test_partial_unparsable_query_in_sqlfluff():
    with pytest.raises(InvalidSyntaxException):
        LineageRunner("SELECT * FROM tab1 AS FULL FULL OUTER JOIN tab2")._eval()
