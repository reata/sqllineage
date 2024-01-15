from sqllineage.utils.entities import ColumnQualifierTuple
from ...helpers import assert_column_lineage_equal


def test_select_column_in_subquery():
    sql = """INSERT INTO tab1
SELECT col1
FROM (SELECT col1 FROM tab2) dt"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("col1", "tab2"), ColumnQualifierTuple("col1", "tab1"))],
    )
    sql = """INSERT INTO tab1
SELECT col1
FROM (SELECT col1, col2 FROM tab2) dt"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("col1", "tab2"), ColumnQualifierTuple("col1", "tab1"))],
    )
    sql = """INSERT INTO tab1
SELECT col1
FROM (SELECT col1 FROM tab2)"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("col1", "tab2"), ColumnQualifierTuple("col1", "tab1"))],
    )


def test_select_column_in_subquery_with_two_parenthesis():
    sql = """INSERT INTO tab1
SELECT col1
FROM ((SELECT col1 FROM tab2)) dt"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("col1", "tab2"), ColumnQualifierTuple("col1", "tab1"))],
    )


def test_select_column_in_subquery_with_two_parenthesis_and_blank_in_between():
    sql = """INSERT INTO tab1
SELECT col1
FROM (
(SELECT col1 FROM tab2)
) dt"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("col1", "tab2"), ColumnQualifierTuple("col1", "tab1"))],
    )


def test_select_column_in_subquery_with_two_parenthesis_and_union():
    sql = """INSERT INTO tab1
SELECT col1
FROM (
    (SELECT col1 FROM tab2)
    UNION ALL
    (SELECT col1 FROM tab3)
) dt"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("col1", "tab1"),
            ),
            (
                ColumnQualifierTuple("col1", "tab3"),
                ColumnQualifierTuple("col1", "tab1"),
            ),
        ],
    )


def test_select_column_in_subquery_with_two_parenthesis_and_union_v2():
    sql = """INSERT INTO tab1
SELECT col1
FROM (
    SELECT col1 FROM tab2
    UNION ALL
    SELECT col1 FROM tab3
) dt"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("col1", "tab1"),
            ),
            (
                ColumnQualifierTuple("col1", "tab3"),
                ColumnQualifierTuple("col1", "tab1"),
            ),
        ],
    )


def test_window_function_in_subquery():
    sql = """INSERT INTO tab1
SELECT rn FROM (
    SELECT
        row_number() over (partition BY col1, col2) rn
    FROM tab2
) sub
WHERE rn = 1"""
    assert_column_lineage_equal(
        sql,
        [
            (ColumnQualifierTuple("col1", "tab2"), ColumnQualifierTuple("rn", "tab1")),
            (ColumnQualifierTuple("col2", "tab2"), ColumnQualifierTuple("rn", "tab1")),
        ],
    )


def test_select_column_in_subquery_alias_and_qualifier_case_insensitive():
    sql = """INSERT INTO tab1
SELECT DT.col1
FROM (SELECT col1 FROM tab2) dt"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("col1", "tab2"), ColumnQualifierTuple("col1", "tab1"))],
    )
    sql = """INSERT INTO tab1
SELECT dt.col1
FROM (SELECT col1 FROM tab2) DT"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("col1", "tab2"), ColumnQualifierTuple("col1", "tab1"))],
    )
    sql = """INSERT INTO tab1
SELECT Dt.col1
FROM (SELECT col1 FROM tab2) dT"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("col1", "tab2"), ColumnQualifierTuple("col1", "tab1"))],
    )
