import pytest

from ....helpers import assert_table_lineage_equal


@pytest.mark.parametrize("dialect", ["exasol", "mysql", "teradata"])
def test_rename_table(dialect: str):
    """
    https://docs.exasol.com/db/latest/sql/rename.htm
    https://dev.mysql.com/doc/refman/8.0/en/rename-table.html
    https://docs.teradata.com/r/Teradata-Database-SQL-Data-Definition-Language-Syntax-and-Examples/December-2015/Table-Statements/RENAME-TABLE
    """
    assert_table_lineage_equal("rename table tab1 to tab2", dialect=dialect)


@pytest.mark.parametrize("dialect", ["exasol", "mysql", "teradata"])
def test_rename_tables(dialect: str):
    assert_table_lineage_equal(
        "rename table tab1 to tab2, tab3 to tab4", dialect=dialect
    )


@pytest.mark.parametrize("dialect", ["databricks", "sparksql"])
def test_refresh_table(dialect: str):
    assert_table_lineage_equal("refresh table tab1", dialect=dialect)


@pytest.mark.parametrize("dialect", ["databricks", "sparksql"])
def test_cache_table(dialect: str):
    assert_table_lineage_equal("cache table tab1 select * from tab2", dialect=dialect)


@pytest.mark.parametrize("dialect", ["databricks", "sparksql"])
def test_uncache_table(dialect: str):
    assert_table_lineage_equal("uncache table tab1", dialect=dialect)


@pytest.mark.parametrize("dialect", ["databricks", "sparksql"])
def test_uncache_table_if_exists(dialect: str):
    assert_table_lineage_equal("uncache table if exists tab1", dialect=dialect)


@pytest.mark.parametrize("dialect", ["databricks", "sparksql"])
def test_show_create_table(dialect: str):
    assert_table_lineage_equal("show create table tab1", dialect=dialect)
