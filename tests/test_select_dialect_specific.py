import pytest

from .helpers import assert_table_lineage_equal


"""
This test class will contain all the tests for testing 'Select Queries' where the dialect is not ANSI.
"""


@pytest.mark.parametrize("dialect", ["bigquery", "mysql"])
def test_select_with_table_name_in_backtick(dialect: str):
    assert_table_lineage_equal("SELECT * FROM `tab1`", {"tab1"}, dialect=dialect)


def test_select_left_semi_join():
    assert_table_lineage_equal(
        "SELECT * FROM tab1 LEFT SEMI JOIN tab2", {"tab1", "tab2"}, dialect="hive"
    )


def test_select_left_semi_join_with_on():
    assert_table_lineage_equal(
        "SELECT * FROM tab1 LEFT SEMI JOIN tab2 ON (tab1.col1 = tab2.col2)",
        {"tab1", "tab2"},
        dialect="hive",
    )


@pytest.mark.parametrize("dialect", ["hive", "mysql", "sparksql"])
def test_select_keyword_as_column_alias(dialect: str):
    # here `as` is the column alias
    assert_table_lineage_equal("SELECT 1 `as` FROM tab1", {"tab1"}, dialect=dialect)
    # the following is hive specific, MySQL doesn't allow this syntax. As of now, we don't test against it
    # helper("SELECT 1 as FROM tab1", {"tab1"})


def test_select_from_generator():
    # generator is Snowflake specific
    sql = """SELECT seq4(), uniform(1, 10, random(12))
FROM table(generator()) v
ORDER BY 1;"""
    assert_table_lineage_equal(sql, dialect="snowflake")
