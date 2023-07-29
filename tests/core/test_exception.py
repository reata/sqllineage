import warnings

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


def test_full_unparsable_query_in_sqlfluff():
    with pytest.raises(InvalidSyntaxException):
        LineageRunner("WRONG SELECT FROM tab1", dialect="ansi")._eval()


def test_partial_unparsable_query_in_sqlfluff():
    with pytest.raises(InvalidSyntaxException):
        LineageRunner(
            "SELECT * FROM tab1 AS FULL FULL OUTER JOIN tab2", dialect="ansi"
        )._eval()


def test_unsupported_query_type_in_sqlfluff():
    with pytest.raises(UnsupportedStatementException):
        LineageRunner(
            "CREATE UNIQUE INDEX title_idx ON films (title)", dialect="ansi"
        )._eval()


def test_deprecated_warning_in_sqlparse():
    with warnings.catch_warnings(record=True) as w:
        LineageRunner("SELECT * FROM DUAL", dialect="non-validating")._eval()
        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)
