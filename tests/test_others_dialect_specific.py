import pytest

from .helpers import assert_table_lineage_equal

"""
This test class will contain all the tests for testing 'Other Queries' where the dialect is not ANSI.
"""


@pytest.mark.parametrize("dialect", ["bigquery", "snowflake"])
def test_create_bucket_table(dialect: str):
    assert_table_lineage_equal(
        "CREATE TABLE tab1 USING parquet CLUSTERED BY (col1) INTO 500 BUCKETS",
        None,
        {"tab1"},
        dialect,
    )


@pytest.mark.parametrize("dialect", ["databricks", "sparksql"])
def test_create_select_without_as(dialect: str):
    assert_table_lineage_equal(
        "CREATE TABLE tab1 SELECT * FROM tab2", {"tab2"}, {"tab1"}, dialect
    )


def test_create_using_serde():
    """
    https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-RowFormats&SerDe
    here with is not an indicator for CTE
    FIXME: sqlfluff hive dialect doesn't support parsing this yet
    """
    # Check
    #
    assert_table_lineage_equal(
        """CREATE TABLE apachelog (
  host STRING,
  identity STRING,
  user STRING,
  time STRING,
  request STRING,
  status STRING,
  size STRING,
  referer STRING,
  agent STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
  "input.regex" = "([^]*) ([^]*) ([^]*) (-|\\[^\\]*\\]) ([^ \"]*|\"[^\"]*\") (-|[0-9]*) (-|[0-9]*)(?: ([^ \"]*|\".*\") ([^ \"]*|\".*\"))?"
)
STORED AS TEXTFILE""",  # noqa
        None,
        {"apachelog"},
        test_sqlfluff=False,
    )


@pytest.mark.parametrize("dialect", ["mysql"])
def test_update_with_join(dialect: str):
    assert_table_lineage_equal(
        "UPDATE tab1 a INNER JOIN tab2 b ON a.col1=b.col1 SET a.col2=b.col2",
        {"tab2"},
        {"tab1"},
        dialect=dialect,
    )


@pytest.mark.parametrize("dialect", ["exasol", "mysql", "teradata"])
def test_rename_table(dialect: str):
    """
    https://docs.exasol.com/db/latest/sql/rename.htm
    https://dev.mysql.com/doc/refman/8.0/en/rename-table.html
    https://docs.teradata.com/r/Teradata-Database-SQL-Data-Definition-Language-Syntax-and-Examples/December-2015/Table-Statements/RENAME-TABLE
    """
    assert_table_lineage_equal("rename table tab1 to tab2", None, None, dialect)


@pytest.mark.parametrize("dialect", ["exasol", "mysql", "teradata"])
def test_rename_tables(dialect: str):
    assert_table_lineage_equal(
        "rename table tab1 to tab2, tab3 to tab4", None, None, dialect
    )


@pytest.mark.parametrize("dialect", ["hive"])
def test_alter_table_exchange_partition(dialect: str):
    """
    See https://cwiki.apache.org/confluence/display/Hive/Exchange+Partition for language manual
    """
    assert_table_lineage_equal(
        "alter table tab1 exchange partition(pt='part1') with table tab2",
        {"tab2"},
        {"tab1"},
        dialect=dialect,
    )


@pytest.mark.parametrize("dialect", ["databricks", "sparksql"])
def test_refresh_table(dialect: str):
    assert_table_lineage_equal("refresh table tab1", None, None, dialect)


@pytest.mark.parametrize("dialect", ["databricks", "sparksql"])
def test_cache_table(dialect: str):
    assert_table_lineage_equal(
        "cache table tab1 select * from tab2", None, None, dialect
    )


@pytest.mark.parametrize("dialect", ["databricks", "sparksql"])
def test_uncache_table(dialect: str):
    assert_table_lineage_equal("uncache table tab1", None, None, dialect)


@pytest.mark.parametrize("dialect", ["databricks", "sparksql"])
def test_uncache_table_if_exists(dialect: str):
    assert_table_lineage_equal("uncache table if exists tab1", None, None, dialect)


@pytest.mark.parametrize("dialect", ["databricks", "hive", "sparksql"])
def test_lateral_view_using_json_tuple(dialect: str):
    sql = """INSERT OVERWRITE TABLE foo
SELECT sc.id, q.item0, q.item1
FROM bar sc
LATERAL VIEW json_tuple(sc.json, 'key1', 'key2') q AS item0, item1"""
    assert_table_lineage_equal(sql, {"bar"}, {"foo"}, dialect)


@pytest.mark.parametrize("dialect", ["databricks", "hive", "sparksql"])
def test_lateral_view_outer(dialect: str):
    sql = """INSERT OVERWRITE TABLE foo
SELECT sc.id, q.col1
FROM bar sc
LATERAL VIEW OUTER explode(sc.json_array) q AS col1"""
    assert_table_lineage_equal(sql, {"bar"}, {"foo"}, dialect)


@pytest.mark.parametrize("dialect", ["databricks", "sparksql"])
def test_show_create_table(dialect: str):
    assert_table_lineage_equal("show create table tab1", None, None, dialect)
