import os
from unittest.mock import patch

import pytest

from sqllineage import SQLPARSE_DIALECT
from sqllineage.exceptions import (
    InvalidSyntaxException,
    SQLLineageException,
    UnsupportedStatementException,
)
from sqllineage.runner import LineageRunner


def test_select_without_table():
    with pytest.raises(SQLLineageException):
        LineageRunner("select * from where foo='bar'")._eval()
    with pytest.raises(SQLLineageException):
        LineageRunner("select * from where foo='bar'", dialect=SQLPARSE_DIALECT)._eval()


def test_full_unparsable_query_in_sqlfluff():
    with pytest.raises(InvalidSyntaxException):
        LineageRunner("WRONG SELECT FROM tab1")._eval()


def test_partial_unparsable_query_in_sqlfluff():
    with pytest.raises(InvalidSyntaxException):
        LineageRunner("SELECT * FROM tab1 AS FULL FULL OUTER JOIN tab2")._eval()


def test_partial_unparsable_query_in_sqlfluff_with_tsql_batch():
    sql = """SELECT *
INTO tgt
FROM tab1 src1 AS src1
CROSS JOIN tab2 AS src2"""
    with pytest.raises(InvalidSyntaxException):
        LineageRunner(sql, dialect="tsql")._eval()


def test_unsupported_query_type_in_sqlfluff():
    with pytest.raises(UnsupportedStatementException):
        LineageRunner("CREATE UNIQUE INDEX title_idx ON films (title)")._eval()


def test_deprecation_warning_in_sqlparse():
    with pytest.warns(DeprecationWarning):
        LineageRunner("SELECT * FROM DUAL", dialect=SQLPARSE_DIALECT)._eval()


def test_syntax_warning_no_semicolon_in_tsql():
    with pytest.warns(SyntaxWarning):
        LineageRunner(
            """SELECT * FROM foo
SELECT * FROM bar""",
            dialect="tsql",
        )._eval()


@patch.dict(os.environ, {"SQLLINEAGE_TSQL_NO_SEMICOLON": "TRUE"})
def test_user_warning_enable_tsql_no_semicolon_with_other_dialect():
    with pytest.warns(UserWarning):
        LineageRunner(
            """SELECT * FROM foo;
SELECT * FROM bar""",
        )._eval()
