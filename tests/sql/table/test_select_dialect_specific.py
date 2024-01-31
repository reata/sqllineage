import pytest

from ...helpers import assert_table_lineage_equal


"""
This test class will contain all the tests for testing 'Select Queries' where the dialect is not ANSI.
"""


@pytest.mark.parametrize(
    "dialect", ["athena", "bigquery", "databricks", "hive", "mysql", "sparksql"]
)
def test_select_with_table_name_in_backtick(dialect: str):
    assert_table_lineage_equal("SELECT * FROM `tab1`", {"tab1"}, dialect=dialect)


@pytest.mark.parametrize(
    "dialect", ["athena", "bigquery", "databricks", "hive", "mysql", "sparksql"]
)
def test_select_with_schema_in_backtick(dialect: str):
    assert_table_lineage_equal(
        "SELECT col1 FROM `schema1`.`tab1`", {"schema1.tab1"}, dialect=dialect
    )


@pytest.mark.parametrize("dialect", ["databricks", "hive", "sparksql"])
def test_select_left_semi_join(dialect: str):
    assert_table_lineage_equal(
        "SELECT * FROM tab1 LEFT SEMI JOIN tab2", {"tab1", "tab2"}, dialect=dialect
    )


@pytest.mark.parametrize("dialect", ["databricks", "hive", "sparksql"])
def test_select_left_semi_join_with_on(dialect: str):
    assert_table_lineage_equal(
        "SELECT * FROM tab1 LEFT SEMI JOIN tab2 ON (tab1.col1 = tab2.col2)",
        {"tab1", "tab2"},
        dialect=dialect,
    )


@pytest.mark.parametrize("dialect", ["snowflake"])
def test_select_from_generator(dialect: str):
    # generator is Snowflake specific
    sql = """SELECT seq4(), uniform(1, 10, random(12))
FROM table(generator()) v
ORDER BY 1;"""
    assert_table_lineage_equal(sql, dialect=dialect)


@pytest.mark.parametrize("dialect", ["postgres", "redshift", "tsql"])
def test_select_into(dialect: str):
    """
    postgres: https://www.postgresql.org/docs/current/sql-selectinto.html
    redshift: https://docs.aws.amazon.com/redshift/latest/dg/r_SELECT_INTO.html
    tsql: https://learn.microsoft.com/en-us/sql/t-sql/queries/select-into-clause-transact-sql?view=sql-server-ver16
    """
    sql = "SELECT * INTO films_recent FROM films WHERE date_prod >= '2002-01-01'"
    assert_table_lineage_equal(sql, {"films"}, {"films_recent"}, dialect=dialect)


@pytest.mark.parametrize("dialect", ["postgres", "tsql"])
def test_select_into_with_union(dialect: str):
    sql = "SELECT * INTO films_all FROM films UNION ALL SELECT * FROM films_backup"
    assert_table_lineage_equal(
        sql, {"films", "films_backup"}, {"films_all"}, dialect=dialect
    )


@pytest.mark.parametrize("dialect", ["athena"])
def test_select_from_unnest_with_ordinality(dialect: str):
    """
    https://prestodb.io/docs/current/sql/select.html#unnest
    """
    sql = """
    SELECT numbers, n, a
    FROM (
      VALUES
        (ARRAY[2, 5]),
        (ARRAY[7, 8, 9])
    ) AS x (numbers)
    CROSS JOIN UNNEST(numbers) WITH ORDINALITY AS t (n, a);
    """
    assert_table_lineage_equal(sql, dialect=dialect)
