from sqllineage.utils.entities import ColumnQualifierTuple
from ...helpers import assert_column_lineage_equal


def test_select_column():
    sql = """INSERT INTO tab1
SELECT col1
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("col1", "tab2"), ColumnQualifierTuple("col1", "tab1"))],
    )
    sql = """INSERT INTO tab1
SELECT col1 AS col2
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("col1", "tab2"), ColumnQualifierTuple("col2", "tab1"))],
    )
    sql = """INSERT INTO tab1
SELECT tab2.col1 AS col2
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("col1", "tab2"), ColumnQualifierTuple("col2", "tab1"))],
    )


def test_select_column_wildcard():
    sql = """INSERT INTO tab1
SELECT *
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("*", "tab2"), ColumnQualifierTuple("*", "tab1"))],
    )
    sql = """INSERT INTO tab1
SELECT *
FROM tab2 a
         INNER JOIN tab3 b
                    ON a.id = b.id"""
    assert_column_lineage_equal(
        sql,
        [
            (ColumnQualifierTuple("*", "tab2"), ColumnQualifierTuple("*", "tab1")),
            (ColumnQualifierTuple("*", "tab3"), ColumnQualifierTuple("*", "tab1")),
        ],
    )


def test_select_column_wildcard_with_qualifier():
    sql = """INSERT INTO tab1
SELECT tab2.*
FROM tab2 a
         INNER JOIN tab3 b
                    ON a.id = b.id"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("*", "tab2"), ColumnQualifierTuple("*", "tab1"))],
    )
    sql = """INSERT INTO tab1
SELECT a.*
FROM tab2 a
         INNER JOIN tab3 b
                    ON a.id = b.id"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("*", "tab2"), ColumnQualifierTuple("*", "tab1"))],
    )


def test_select_distinct_column():
    sql = """INSERT INTO tab1
SELECT DISTINCT col1
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("col1", "tab2"), ColumnQualifierTuple("col1", "tab1"))],
    )


def test_select_column_with_table_qualifier():
    sql = """INSERT INTO tab1
SELECT tab2.col1
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("col1", "tab2"), ColumnQualifierTuple("col1", "tab1"))],
    )
    sql = """INSERT INTO tab1
SELECT t.col1
FROM tab2 AS t"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("col1", "tab2"), ColumnQualifierTuple("col1", "tab1"))],
    )


def test_select_column_with_table_qualifier_case_insensitive():
    sql = """INSERT INTO tab1
SELECT TA.col1
FROM tab2 AS ta"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("col1", "tab2"), ColumnQualifierTuple("col1", "tab1"))],
    )
    sql = """INSERT INTO tab1
SELECT ta.col1
FROM tab2 AS TA"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("col1", "tab2"), ColumnQualifierTuple("col1", "tab1"))],
    )
    sql = """INSERT INTO tab1
SELECT Ta.col1
FROM tab2 AS tA"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("col1", "tab2"), ColumnQualifierTuple("col1", "tab1"))],
    )


def test_select_columns():
    sql = """INSERT INTO tab1
SELECT col1,
col2
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("col1", "tab1"),
            ),
            (
                ColumnQualifierTuple("col2", "tab2"),
                ColumnQualifierTuple("col2", "tab1"),
            ),
        ],
    )
    sql = """INSERT INTO tab1
SELECT max(col1),
max(col2)
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("max(col1)", "tab1"),
            ),
            (
                ColumnQualifierTuple("col2", "tab2"),
                ColumnQualifierTuple("max(col2)", "tab1"),
            ),
        ],
    )


def test_comment_after_column_comma_first():
    sql = """INSERT INTO tab1
SELECT a.col1
       --, a.col2
       , a.col3
FROM tab2 a"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("col1", "tab1"),
            ),
            (
                ColumnQualifierTuple("col3", "tab2"),
                ColumnQualifierTuple("col3", "tab1"),
            ),
        ],
    )


def test_comment_after_column_comma_last():
    sql = """INSERT INTO tab1
SELECT a.col1,
       -- a.col2,
       a.col3
FROM tab2 a"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("col1", "tab1"),
            ),
            (
                ColumnQualifierTuple("col3", "tab2"),
                ColumnQualifierTuple("col3", "tab1"),
            ),
        ],
    )


def test_select_column_name_case_insensitive():
    sql = """INSERT INTO tab1
SELECT col1,
COL1
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("col1", "tab1"),
            )
        ],
    )


def test_non_reserved_keyword_as_column_name():
    sql = """INSERT INTO tab1
SELECT host
FROM tab2 a"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("host", "tab2"),
                ColumnQualifierTuple("host", "tab1"),
            )
        ],
        test_sqlparse=False,
    )


def test_current_timestamp():
    """
    current_timestamp is a keyword since ANSI SQL-2016
    sqlparse cannot produce the correct AST for this case
    """
    sql = """INSERT INTO tab1
SELECT current_timestamp as col1,
       col2,
       col3
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col2", "tab2"),
                ColumnQualifierTuple("col2", "tab1"),
            ),
            (
                ColumnQualifierTuple("col3", "tab2"),
                ColumnQualifierTuple("col3", "tab1"),
            ),
        ],
        test_sqlparse=False,
    )
