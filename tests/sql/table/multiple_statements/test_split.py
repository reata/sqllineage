import pytest

from sqllineage.core.parser.sqlfluff.analyzer import SqlFluffLineageAnalyzer
from sqllineage.core.parser.sqlparse.analyzer import SqlParseLineageAnalyzer


def _n_statements(analyzer_cls, sql: str) -> int:
    return len(analyzer_cls(sql, file_path=".", dialect="ansi").statements)


@pytest.mark.parametrize(
    "analyzer_cls", [SqlFluffLineageAnalyzer, SqlParseLineageAnalyzer]
)
def test_split_empty_sql(analyzer_cls):
    assert _n_statements(analyzer_cls, "") == 0
    assert _n_statements(analyzer_cls, "   ") == 0


@pytest.mark.parametrize(
    "analyzer_cls", [SqlFluffLineageAnalyzer, SqlParseLineageAnalyzer]
)
def test_split_statements(analyzer_cls):
    assert _n_statements(analyzer_cls, "SELECT * FROM tab1; SELECT * FROM tab2;") == 2


@pytest.mark.parametrize(
    "analyzer_cls", [SqlFluffLineageAnalyzer, SqlParseLineageAnalyzer]
)
def test_split_statements_with_heading_and_ending_new_line(analyzer_cls):
    assert (
        _n_statements(analyzer_cls, "\nSELECT * FROM tab1;\nSELECT * FROM tab2;\n") == 2
    )


@pytest.mark.parametrize(
    "analyzer_cls", [SqlFluffLineageAnalyzer, SqlParseLineageAnalyzer]
)
def test_split_statements_with_comment(analyzer_cls):
    sql = """SELECT 1;

-- SELECT 2;"""
    assert _n_statements(analyzer_cls, sql) == 1


@pytest.mark.parametrize(
    "analyzer_cls", [SqlFluffLineageAnalyzer, SqlParseLineageAnalyzer]
)
def test_split_statement_ends_with_multiple_semicolons(analyzer_cls):
    assert _n_statements(analyzer_cls, "SELECT 1;;;") == 1


# SHOW CREATE TABLE and DESC are not ANSI standard — sqlfluff does not parse them.
# These are sqlparse-specific edge cases.
@pytest.mark.parametrize("analyzer_cls", [SqlParseLineageAnalyzer])
def test_split_statements_with_show_create_table(analyzer_cls):
    sql = """SELECT 1;

SHOW CREATE TABLE tab1;"""
    assert _n_statements(analyzer_cls, sql) == 2


@pytest.mark.parametrize("analyzer_cls", [SqlParseLineageAnalyzer])
def test_split_statements_with_desc(analyzer_cls):
    sql = """SELECT 1;

DESC tab1;"""
    assert _n_statements(analyzer_cls, sql) == 2
