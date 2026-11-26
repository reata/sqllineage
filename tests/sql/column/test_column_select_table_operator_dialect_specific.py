import pytest

from sqllineage.utils.entities import ColumnQualifierTuple

from ...helpers import assert_column_lineage_equal

# the alias after PIVOT/UNPIVOT names the pivoted dataset, so columns qualified by it come from the table pivoted
_pivot_dialects = [
    "bigquery",
    "databricks",
    "duckdb",
    "oracle",
    "redshift",
    "snowflake",
    "sparksql",
]


@pytest.mark.parametrize("dialect", _pivot_dialects + ["tsql"])
def test_select_column_from_unpivot_with_alias(dialect: str):
    sql = """INSERT INTO tab1
SELECT u.id, u.quarter, u.sales
FROM tab2 UNPIVOT (sales FOR quarter IN (q1, q2)) AS u"""
    assert_column_lineage_equal(
        sql,
        [
            (ColumnQualifierTuple("id", "tab2"), ColumnQualifierTuple("id", "tab1")),
            (
                ColumnQualifierTuple("quarter", "tab2"),
                ColumnQualifierTuple("quarter", "tab1"),
            ),
            (
                ColumnQualifierTuple("sales", "tab2"),
                ColumnQualifierTuple("sales", "tab1"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", _pivot_dialects + ["tsql"])
def test_select_column_from_aliased_table_unpivot_with_alias(dialect: str):
    sql = """INSERT INTO tab1
SELECT u.id, o.name
FROM tab2 AS t UNPIVOT (sales FOR quarter IN (q1, q2)) AS u
JOIN tab3 AS o ON u.id = o.id"""
    assert_column_lineage_equal(
        sql,
        [
            (ColumnQualifierTuple("id", "tab2"), ColumnQualifierTuple("id", "tab1")),
            (
                ColumnQualifierTuple("name", "tab3"),
                ColumnQualifierTuple("name", "tab1"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", _pivot_dialects)
def test_select_column_from_pivot_with_alias(dialect: str):
    sql = """INSERT INTO tab1
SELECT p.id
FROM tab2 PIVOT (sum(amount) FOR month IN ('JAN', 'FEB')) AS p"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("id", "tab2"), ColumnQualifierTuple("id", "tab1"))],
        dialect=dialect,
        test_sqlparse=False,
    )


def test_select_column_from_pivot_with_alias_tsql():
    sql = """INSERT INTO tab1
SELECT p.id
FROM tab2 PIVOT (sum(amount) FOR month IN ([JAN], [FEB])) AS p"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("id", "tab2"), ColumnQualifierTuple("id", "tab1"))],
        dialect="tsql",
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", _pivot_dialects + ["tsql"])
def test_select_column_from_subquery_unpivot_with_alias(dialect: str):
    sql = """INSERT INTO tab1
SELECT u.id
FROM (SELECT id, q1, q2 FROM tab2) AS sq UNPIVOT (sales FOR quarter IN (q1, q2)) AS u"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("id", "tab2"), ColumnQualifierTuple("id", "tab1"))],
        dialect=dialect,
        test_sqlparse=False,
    )


# https://docs.snowflake.com/en/sql-reference/constructs/at-before
# https://docs.snowflake.com/en/sql-reference/constructs/changes
@pytest.mark.parametrize(
    "time_travel",
    [
        "AT(TIMESTAMP => '2024-01-01'::timestamp_ltz)",
        "BEFORE(STATEMENT => '8e5d0ca9-005e-44e6-b858-a8f5b37c5726')",
        "CHANGES(INFORMATION => DEFAULT) AT(OFFSET => -60 * 5)",
    ],
)
def test_select_column_from_time_travel_with_alias_snowflake(time_travel: str):
    sql = f"""INSERT INTO tab1
SELECT t.id
FROM tab2 {time_travel} AS t"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("id", "tab2"), ColumnQualifierTuple("id", "tab1"))],
        dialect="snowflake",
        test_sqlparse=False,
    )


def test_select_column_from_time_travel_with_alias_in_join_snowflake():
    sql = """INSERT INTO tab1
SELECT oldt.id, newt.name
FROM tab2 AT(STATEMENT => '8e5d0ca9-005e-44e6-b858-a8f5b37c5726') oldt
FULL OUTER JOIN tab2 newt ON oldt.id = newt.id
WHERE oldt.id IS NULL OR newt.id IS NULL"""
    assert_column_lineage_equal(
        sql,
        [
            (ColumnQualifierTuple("id", "tab2"), ColumnQualifierTuple("id", "tab1")),
            (
                ColumnQualifierTuple("name", "tab2"),
                ColumnQualifierTuple("name", "tab1"),
            ),
        ],
        dialect="snowflake",
        test_sqlparse=False,
    )
