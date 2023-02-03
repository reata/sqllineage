import pytest

from tests.helpers import assert_table_lineage_equal


"""
This test class will contain all the tests for testing 'CTE Queries' where the dialect is not ANSI.
"""


def test_with_insert_overwrite():
    assert_table_lineage_equal(
        "WITH tab1 AS (SELECT * FROM tab2) INSERT OVERWRITE tab3 SELECT * FROM tab1",
        {"tab2"},
        {"tab3"},
        dialect="sparksql",
    )


@pytest.mark.parametrize("dialect", ["hive", "sparksql"])
def test_with_insert_plus_keyword_table(dialect: str):
    assert_table_lineage_equal(
        "WITH tab1 AS (SELECT * FROM tab2) INSERT INTO TABLE tab3 SELECT * FROM tab1",
        {"tab2"},
        {"tab3"},
        dialect=dialect,
    )


@pytest.mark.parametrize("dialect", ["hive", "sparksql"])
def test_with_insert_overwrite_plus_keyword_table(dialect: str):
    assert_table_lineage_equal(
        "WITH tab1 AS (SELECT * FROM tab2) INSERT OVERWRITE TABLE tab3 SELECT * FROM tab1",
        {"tab2"},
        {"tab3"},
        dialect=dialect,
    )


@pytest.mark.parametrize("dialect", ["mysql", "sparksql"])
def test_with_select_one_without_as(dialect: str):
    # AS in CTE is negligible in SparkSQL, however it is required in MySQL. See below reference
    # https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-cte.html
    # https://dev.mysql.com/doc/refman/8.0/en/with.html
    assert_table_lineage_equal(
        "WITH wtab1 (SELECT * FROM schema1.tab1) SELECT * FROM wtab1",
        {"schema1.tab1"},
        dialect=dialect,
    )
