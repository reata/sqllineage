from sqllineage.utils.entities import ColumnQualifierTuple
from ...helpers import assert_column_lineage_equal


def test_select_column_using_expression():
    sql = """INSERT INTO tab1
SELECT col1 + col2
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("col1 + col2", "tab1"),
            ),
            (
                ColumnQualifierTuple("col2", "tab2"),
                ColumnQualifierTuple("col1 + col2", "tab1"),
            ),
        ],
    )
    sql = """INSERT INTO tab1
SELECT col1 + col2 AS col3
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
    )


def test_select_column_multiply_expression():
    sql = """INSERT INTO tab1
SELECT col1 * 1 AS col1
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("col1", "tab1"),
            ),
        ],
    )


def test_select_column_using_expression_in_parenthesis():
    sql = """INSERT INTO tab1
SELECT (col1 + col2)
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("(col1 + col2)", "tab1"),
            ),
            (
                ColumnQualifierTuple("col2", "tab2"),
                ColumnQualifierTuple("(col1 + col2)", "tab1"),
            ),
        ],
    )
    sql = """INSERT INTO tab1
SELECT (col1 + col2) AS col3
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
    )


def test_select_column_using_boolean_expression_in_parenthesis():
    sql = """INSERT INTO tab1
SELECT (col1 > 0 AND col2 > 0) AS col3
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
    )


def test_select_column_using_expression_with_table_qualifier_without_column_alias():
    sql = """INSERT INTO tab1
SELECT a.col1 + a.col2 + a.col3 + a.col4
FROM tab2 a"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("a.col1 + a.col2 + a.col3 + a.col4", "tab1"),
            ),
            (
                ColumnQualifierTuple("col2", "tab2"),
                ColumnQualifierTuple("a.col1 + a.col2 + a.col3 + a.col4", "tab1"),
            ),
            (
                ColumnQualifierTuple("col3", "tab2"),
                ColumnQualifierTuple("a.col1 + a.col2 + a.col3 + a.col4", "tab1"),
            ),
            (
                ColumnQualifierTuple("col4", "tab2"),
                ColumnQualifierTuple("a.col1 + a.col2 + a.col3 + a.col4", "tab1"),
            ),
        ],
    )


def test_subquery_expression_without_source_table():
    assert_column_lineage_equal(
        """INSERT INTO foo
SELECT (SELECT col1 + col2 AS result) AS sum_result
FROM bar""",
        [
            (
                ColumnQualifierTuple("col1", "bar"),
                ColumnQualifierTuple("sum_result", "foo"),
            ),
            (
                ColumnQualifierTuple("col2", "bar"),
                ColumnQualifierTuple("sum_result", "foo"),
            ),
        ],
    )
