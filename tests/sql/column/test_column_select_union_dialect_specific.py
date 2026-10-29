import pytest

from sqllineage.utils.entities import ColumnQualifierTuple

from ...helpers import assert_column_lineage_equal


@pytest.mark.parametrize("dialect", ["bigquery", "duckdb", "snowflake"])
def test_union_all_by_name(dialect: str):
    """
    UNION ALL BY NAME matches columns by name instead of by position
    """
    sql = """INSERT INTO tab3
SELECT col1, col2
FROM tab1
UNION ALL BY NAME
SELECT col2, col1
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab1"),
                ColumnQualifierTuple("col1", "tab3"),
            ),
            (
                ColumnQualifierTuple("col2", "tab1"),
                ColumnQualifierTuple("col2", "tab3"),
            ),
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("col1", "tab3"),
            ),
            (
                ColumnQualifierTuple("col2", "tab2"),
                ColumnQualifierTuple("col2", "tab3"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", ["duckdb", "snowflake"])
def test_union_by_name(dialect: str):
    sql = """CREATE TABLE tab3 AS
SELECT col1, col2 AS col3
FROM tab1
UNION BY NAME
SELECT col4 AS col3, col1
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab1"),
                ColumnQualifierTuple("col1", "tab3"),
            ),
            (
                ColumnQualifierTuple("col2", "tab1"),
                ColumnQualifierTuple("col3", "tab3"),
            ),
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("col1", "tab3"),
            ),
            (
                ColumnQualifierTuple("col4", "tab2"),
                ColumnQualifierTuple("col3", "tab3"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", ["bigquery"])
def test_union_all_corresponding(dialect: str):
    sql = """INSERT INTO tab3
SELECT col1, col2
FROM tab1
UNION ALL CORRESPONDING
SELECT col2, col1
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab1"),
                ColumnQualifierTuple("col1", "tab3"),
            ),
            (
                ColumnQualifierTuple("col2", "tab1"),
                ColumnQualifierTuple("col2", "tab3"),
            ),
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("col1", "tab3"),
            ),
            (
                ColumnQualifierTuple("col2", "tab2"),
                ColumnQualifierTuple("col2", "tab3"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", ["duckdb", "snowflake"])
def test_union_all_by_name_with_different_columns(dialect: str):
    """
    a column missing from one query is filled with NULL, and a column only in later queries is appended to the result
    """
    sql = """INSERT INTO tab4
SELECT col1
FROM tab1
UNION ALL BY NAME
SELECT col2, col1
FROM tab2
UNION ALL BY NAME
SELECT col2, col3
FROM tab3"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab1"),
                ColumnQualifierTuple("col1", "tab4"),
            ),
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("col1", "tab4"),
            ),
            (
                ColumnQualifierTuple("col2", "tab2"),
                ColumnQualifierTuple("col2", "tab4"),
            ),
            (
                ColumnQualifierTuple("col2", "tab3"),
                ColumnQualifierTuple("col2", "tab4"),
            ),
            (
                ColumnQualifierTuple("col3", "tab3"),
                ColumnQualifierTuple("col3", "tab4"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", ["duckdb", "snowflake"])
def test_union_all_by_name_with_column_specified_in_dml(dialect: str):
    """
    the result of UNION ALL BY NAME is inserted into the columns specified in DML by position
    """
    sql = """INSERT INTO tab3 (col4, col5, col6)
SELECT 1 AS col1, col2, col3
FROM tab1
UNION ALL BY NAME
SELECT col3, col2, col1
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col2", "tab1"),
                ColumnQualifierTuple("col5", "tab3"),
            ),
            (
                ColumnQualifierTuple("col3", "tab1"),
                ColumnQualifierTuple("col6", "tab3"),
            ),
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("col4", "tab3"),
            ),
            (
                ColumnQualifierTuple("col2", "tab2"),
                ColumnQualifierTuple("col5", "tab3"),
            ),
            (
                ColumnQualifierTuple("col3", "tab2"),
                ColumnQualifierTuple("col6", "tab3"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", ["duckdb", "snowflake"])
def test_union_all_by_name_in_subquery(dialect: str):
    sql = """INSERT INTO tab3
SELECT sq.col1
FROM (SELECT col1, col2
      FROM tab1
      UNION ALL BY NAME
      SELECT col2, col1
      FROM tab2) sq"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab1"),
                ColumnQualifierTuple("col1", "tab3"),
            ),
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("col1", "tab3"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )
