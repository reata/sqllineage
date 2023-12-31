from sqllineage.utils.entities import ColumnQualifierTuple
from ...helpers import assert_column_lineage_equal


def test_select_column_using_function():
    sql = """INSERT INTO tab1
SELECT max(col1),
       count(*)
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("max(col1)", "tab1"),
            ),
            (
                ColumnQualifierTuple("*", "tab2"),
                ColumnQualifierTuple("count(*)", "tab1"),
            ),
        ],
    )
    sql = """INSERT INTO tab1
SELECT max(col1) AS col2,
       count(*)  AS cnt
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("col2", "tab1"),
            ),
            (ColumnQualifierTuple("*", "tab2"), ColumnQualifierTuple("cnt", "tab1")),
        ],
    )


def test_select_column_using_function_with_complex_parameter():
    sql = """INSERT INTO tab1
SELECT if(col1 = 'foo' AND col2 = 'bar', 1, 0) AS flag
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("flag", "tab1"),
            ),
            (
                ColumnQualifierTuple("col2", "tab2"),
                ColumnQualifierTuple("flag", "tab1"),
            ),
        ],
    )


def test_select_column_using_window_function():
    sql = """INSERT INTO tab1
SELECT row_number() over (partition BY col1 ORDER BY col2 DESC) AS rnum
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("rnum", "tab1"),
            ),
            (
                ColumnQualifierTuple("col2", "tab2"),
                ColumnQualifierTuple("rnum", "tab1"),
            ),
        ],
    )


def test_select_column_using_window_function_with_parameters():
    sql = """INSERT INTO tab1
SELECT col0,
       max(col3) over (partition BY col1 ORDER BY col2 DESC) AS rnum,
       col4
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col0", "tab2"),
                ColumnQualifierTuple("col0", "tab1"),
            ),
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("rnum", "tab1"),
            ),
            (
                ColumnQualifierTuple("col2", "tab2"),
                ColumnQualifierTuple("rnum", "tab1"),
            ),
            (
                ColumnQualifierTuple("col3", "tab2"),
                ColumnQualifierTuple("rnum", "tab1"),
            ),
            (
                ColumnQualifierTuple("col4", "tab2"),
                ColumnQualifierTuple("col4", "tab1"),
            ),
        ],
    )


def test_column_with_ctas_and_func():
    sql = """CREATE TABLE tab2 AS
SELECT coalesce(col1, 0)             AS col1,
       if(col1 IS NOT NULL, 1, NULL) AS col2
FROM tab1"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab1"),
                ColumnQualifierTuple("col1", "tab2"),
            ),
            (
                ColumnQualifierTuple("col1", "tab1"),
                ColumnQualifierTuple("col2", "tab2"),
            ),
        ],
    )


def test_coalesce_with_whitespace():
    """
    coalesce is a keyword since ANSI SQL-2023
    usually it's parsed as a function. however, when whitespace followed which is valid syntax,
    sqlparse cannot produce the correct AST
    """
    sql = """INSERT INTO tab1
SELECT coalesce (col1, col2) as col3
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("col3", "tab1"),
            ),
            (
                ColumnQualifierTuple("col2", "tab2"),
                ColumnQualifierTuple("col3", "tab1"),
            ),
        ],
        test_sqlparse=False,
    )
