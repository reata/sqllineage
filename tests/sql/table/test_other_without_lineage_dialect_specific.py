import pytest

from ...helpers import assert_table_lineage_equal


@pytest.mark.parametrize("dialect", ["exasol", "mysql", "teradata"])
def test_rename_table(dialect: str):
    """
    https://docs.exasol.com/db/latest/sql/rename.htm
    https://dev.mysql.com/doc/refman/8.0/en/rename-table.html
    https://docs.teradata.com/r/Teradata-Database-SQL-Data-Definition-Language-Syntax-and-Examples/December-2015/Table-Statements/RENAME-TABLE
    """
    assert_table_lineage_equal("rename table tab1 to tab2", dialect=dialect)


@pytest.mark.parametrize("dialect", ["mysql"])
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


@pytest.mark.parametrize("dialect", ["databricks", "sparksql"])
def test_add_jar(dialect: str):
    assert_table_lineage_equal("ADD JAR /tmp/SimpleUdf.jar", dialect=dialect)


@pytest.mark.parametrize("dialect", ["databricks", "sparksql"])
def test_create_function(dialect: str):
    assert_table_lineage_equal(
        """CREATE FUNCTION simple_udf AS 'SimpleUdf'
  USING JAR '/tmp/SimpleUdf.jar'""",
        dialect=dialect,
    )


@pytest.mark.parametrize("dialect", ["databricks", "sparksql"])
def test_create_or_replace_function(dialect: str):
    assert_table_lineage_equal(
        """CREATE OR REPLACE FUNCTION simple_udf AS 'SimpleUdf'
      USING JAR '/tmp/SimpleUdf.jar'""",
        dialect=dialect,
    )


@pytest.mark.parametrize("dialect", ["databricks", "sparksql"])
def test_create_temporary_function(dialect: str):
    assert_table_lineage_equal(
        """CREATE TEMPORARY FUNCTION simple_temp_udf AS 'SimpleUdf'
  USING JAR '/tmp/SimpleUdf.jar'""",
        dialect=dialect,
    )


@pytest.mark.parametrize("dialect", ["databricks", "sparksql"])
def test_create_or_replace_temporary_function(dialect: str):
    assert_table_lineage_equal(
        """CREATE OR REPLACE TEMPORARY FUNCTION simple_temp_udf AS 'SimpleUdf'
  USING JAR '/tmp/SimpleUdf.jar'""",
        dialect=dialect,
    )


@pytest.mark.parametrize("dialect", ["databricks", "sparksql"])
def test_show_functions(dialect: str):
    assert_table_lineage_equal("SHOW FUNCTIONS trim", dialect=dialect)


@pytest.mark.parametrize("dialect", ["databricks", "sparksql"])
def test_describe_function(dialect: str):
    assert_table_lineage_equal("DESC FUNCTION abs", dialect=dialect)


@pytest.mark.parametrize("dialect", ["databricks", "sparksql"])
def test_drop_function(dialect: str):
    assert_table_lineage_equal("DROP FUNCTION test_avg", dialect=dialect)


@pytest.mark.parametrize("dialect", ["databricks", "hive", "sparksql"])
def test_set_command(dialect: str):
    assert_table_lineage_equal(
        "SET spark.sql.variable.substitute=false", dialect=dialect
    )


@pytest.mark.parametrize("dialect", ["databricks", "sparksql"])
def test_set_command_without_property_value(dialect: str):
    assert_table_lineage_equal("SET spark.sql.variable.substitute", dialect=dialect)


@pytest.mark.parametrize("dialect", ["postgres", "redshift"])
def test_analyze_table(dialect: str):
    assert_table_lineage_equal("analyze tab", dialect=dialect)


@pytest.mark.parametrize("dialect", ["postgres", "redshift"])
def test_analyze_table_column(dialect: str):
    assert_table_lineage_equal("analyze tab (col1, col2)", dialect=dialect)
