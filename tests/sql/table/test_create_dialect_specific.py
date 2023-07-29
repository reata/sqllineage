import pytest

from ...helpers import assert_table_lineage_equal


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


@pytest.mark.parametrize("dialect", ["duckdb", "greenplum", "postgres", "redshift"])
def test_create_table_as_with_postgres_dialect(dialect: str):
    """
    sqlfluff postgres family dialects parse CTAS statement as "create_table_as_statement",
    unlike "create_table_statement" in ansi dialect
    """
    assert_table_lineage_equal(
        """CREATE TABLE bar AS
SELECT *
FROM foo""",
        {"foo"},
        {"bar"},
        dialect,
    )


@pytest.mark.parametrize("dialect", ["databricks", "hive", "sparksql"])
def test_create_using_serde(dialect: str):
    """
    https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-RowFormats&SerDe
    here with is not an indicator for CTE
    """
    # use raw sql string to avoid Python recognize some character as escaping
    assert_table_lineage_equal(
        r"""CREATE TABLE apachelog (
  host STRING,
  identity STRING,
  `user` STRING,
  `time` STRING,
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
        dialect=dialect,
    )
