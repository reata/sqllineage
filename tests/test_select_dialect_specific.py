import pytest

from .helpers import assert_table_lineage_equal


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


def test_select_from_generator():
    # generator is Snowflake specific
    sql = """SELECT seq4(), uniform(1, 10, random(12))
FROM table(generator()) v
ORDER BY 1;"""
    assert_table_lineage_equal(sql, dialect="snowflake")
