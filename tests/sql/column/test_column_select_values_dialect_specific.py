import pytest

from sqllineage.utils.entities import ColumnQualifierTuple

from ...helpers import assert_column_lineage_equal


@pytest.mark.parametrize("dialect", ["tsql"])
def test_select_column_from_cross_apply_values(dialect: str):
    sql = """INSERT INTO tab1 (id, col1, col2)
SELECT s.id, v.col1, v.col2
FROM tab2 s
CROSS APPLY (VALUES (s.col_a1, s.col_a2), (s.col_b1, s.col_b2)) v (col1, col2)"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("id", "tab2"),
                ColumnQualifierTuple("id", "tab1"),
            ),
            (
                ColumnQualifierTuple("col_a1", "tab2"),
                ColumnQualifierTuple("col1", "tab1"),
            ),
            (
                ColumnQualifierTuple("col_b1", "tab2"),
                ColumnQualifierTuple("col1", "tab1"),
            ),
            (
                ColumnQualifierTuple("col_a2", "tab2"),
                ColumnQualifierTuple("col2", "tab1"),
            ),
            (
                ColumnQualifierTuple("col_b2", "tab2"),
                ColumnQualifierTuple("col2", "tab1"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", ["tsql"])
def test_select_unqualified_column_from_outer_apply_values(dialect: str):
    sql = """INSERT INTO tab1 (id, val)
SELECT s.id, v.val
FROM tab2 s
OUTER APPLY (VALUES (col_a), (col_b + 1)) AS v (val)"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("id", "tab2"),
                ColumnQualifierTuple("id", "tab1"),
            ),
            (
                ColumnQualifierTuple("col_a", "tab2"),
                ColumnQualifierTuple("val", "tab1"),
            ),
            (
                ColumnQualifierTuple("col_b", "tab2"),
                ColumnQualifierTuple("val", "tab1"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", ["postgres", "tsql"])
def test_select_column_from_lateral_values_with_literal(dialect: str):
    lateral = "CROSS APPLY" if dialect == "tsql" else "CROSS JOIN LATERAL"
    sql = f"""INSERT INTO tab1
SELECT s.id, v.val
FROM tab2 s
{lateral} (VALUES ('a', s.col_a), ('b', s.col_b)) AS v (k, val)"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("id", "tab2"),
                ColumnQualifierTuple("id", "tab1"),
            ),
            (
                ColumnQualifierTuple("col_a", "tab2"),
                ColumnQualifierTuple("val", "tab1"),
            ),
            (
                ColumnQualifierTuple("col_b", "tab2"),
                ColumnQualifierTuple("val", "tab1"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", ["tsql"])
def test_select_column_from_cross_apply_table_function(dialect: str):
    sql = """INSERT INTO tab1 (id, col1, col2)
SELECT s.id, x.col1, x.col2
FROM tab2 s
CROSS APPLY my_function(s.col_a) x"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("id", "tab2"),
                ColumnQualifierTuple("id", "tab1"),
            ),
            (
                ColumnQualifierTuple("col_a", "tab2"),
                ColumnQualifierTuple("col1", "tab1"),
            ),
            (
                ColumnQualifierTuple("col_a", "tab2"),
                ColumnQualifierTuple("col2", "tab1"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )
