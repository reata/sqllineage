import pytest

from ...helpers import assert_table_lineage_equal

"""
This test class will contain all the tests for testing 'CTE Queries' where the dialect is not ANSI.
"""


@pytest.mark.parametrize("dialect", ["databricks", "hive", "sparksql"])
def test_with_insert_plus_table_keyword(dialect: str):
    assert_table_lineage_equal(
        "WITH tab1 AS (SELECT * FROM tab2) INSERT INTO TABLE tab3 SELECT * FROM tab1",
        {"tab2"},
        {"tab3"},
        dialect=dialect,
    )


@pytest.mark.parametrize("dialect", ["databricks", "hive", "sparksql"])
def test_with_insert_overwrite(dialect: str):
    assert_table_lineage_equal(
        "WITH tab1 AS (SELECT * FROM tab2) INSERT OVERWRITE TABLE tab3 SELECT * FROM tab1",
        {"tab2"},
        {"tab3"},
        dialect=dialect,
    )


@pytest.mark.parametrize("dialect", ["databricks", "sparksql"])
def test_with_insert_overwrite_without_table_keyword(dialect: str):
    assert_table_lineage_equal(
        "WITH tab1 AS (SELECT * FROM tab2) INSERT OVERWRITE tab3 SELECT * FROM tab1",
        {"tab2"},
        {"tab3"},
        dialect=dialect,
    )


@pytest.mark.parametrize("dialect", ["databricks", "sparksql"])
def test_with_select_one_without_as(dialect: str):
    # AS in CTE is negligible in SparkSQL, however it is required in most other dialects
    # https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-cte.html
    # https://dev.mysql.com/doc/refman/8.0/en/with.html
    assert_table_lineage_equal(
        "WITH wtab1 (SELECT * FROM schema1.tab1) SELECT * FROM wtab1",
        {"schema1.tab1"},
        dialect=dialect,
    )
