import pytest

from sqllineage.utils.entities import ColumnQualifierTuple
from ...helpers import assert_column_lineage_equal


def test_select_column_using_cast():
    sql = """INSERT INTO tab1
SELECT cast(col1 AS timestamp)
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("cast(col1 as timestamp)", "tab1"),
            )
        ],
    )
    sql = """INSERT INTO tab1
SELECT cast(col1 AS timestamp) AS col2
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("col1", "tab2"), ColumnQualifierTuple("col2", "tab1"))],
    )


def test_cast_with_comparison():
    sql = """INSERT INTO tab1
SELECT cast(col1 = 1 AS int) col1, col2 = col3 col2
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
            (
                ColumnQualifierTuple("col3", "tab2"),
                ColumnQualifierTuple("col2", "tab1"),
            ),
        ],
    )


@pytest.mark.parametrize(
    "dtype", ["string", "timestamp", "date", "decimal(18, 0)", "varchar(255)"]
)
def test_cast_to_data_type(dtype: str):
    sql = f"""INSERT INTO tab1
SELECT cast(col1 as {dtype}) AS col1
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("col1", "tab2"), ColumnQualifierTuple("col1", "tab1"))],
    )


@pytest.mark.parametrize(
    "dtype", ["string", "timestamp", "date", "decimal(18, 0)", "varchar(255)"]
)
def test_nested_cast_to_data_type(dtype: str):
    sql = f"""INSERT INTO tab1
SELECT cast(cast(col1 AS {dtype}) AS {dtype}) AS col1
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("col1", "tab2"), ColumnQualifierTuple("col1", "tab1"))],
    )
    sql = f"""INSERT INTO tab1
SELECT cast(cast(cast(cast(cast(col1 AS {dtype}) AS {dtype}) AS {dtype}) AS {dtype}) AS {dtype}) AS col1
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("col1", "tab2"), ColumnQualifierTuple("col1", "tab1"))],
    )


@pytest.mark.parametrize(
    "dtype", ["string", "timestamp", "date", "decimal(18, 0)", "varchar(255)"]
)
def test_cast_to_data_type_with_case_when(dtype: str):
    sql = f"""INSERT INTO tab1
SELECT cast(case when col1 > 0 then col2 else col3 end as {dtype}) AS col1
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
                ColumnQualifierTuple("col1", "tab1"),
            ),
            (
                ColumnQualifierTuple("col3", "tab2"),
                ColumnQualifierTuple("col1", "tab1"),
            ),
        ],
    )


def test_cast_using_constant():
    sql = """INSERT INTO tab1
SELECT cast('2012-12-21' AS date) AS col2"""
    assert_column_lineage_equal(sql)


def test_postgres_style_type_cast():
    sql = """INSERT INTO tab1
SELECT col1::timestamp
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("col1", "tab2"), ColumnQualifierTuple("col1", "tab1"))],
    )


@pytest.mark.parametrize(
    "func",
    [
        "coalesce(col1, 0) as varchar",
        "if(col1 > 100, 100, col1) as varchar",
        "ln(col1) as varchar",
        "conv(col1, 10, 2) as varchar",
        "ln(cast(coalesce(col1, '0') as int)) as varchar",
        "coalesce(col1, 0) as decimal(10, 6)",
    ],
)
def test_column_try_cast_with_func(func: str):
    sql = f"""INSERT INTO tab2
SELECT try_cast({func}) AS col2
FROM tab1"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab1"),
                ColumnQualifierTuple("col2", "tab2"),
            ),
        ],
    )
