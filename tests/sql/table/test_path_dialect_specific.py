import pytest

from sqllineage.core.models import Path

from ...helpers import assert_table_lineage_equal


@pytest.mark.parametrize("dialect", ["postgres", "redshift"])
def test_copy_from_path(dialect: str):
    """
    https://www.postgresql.org/docs/current/sql-copy.html (Postgres)
    https://docs.aws.amazon.com/es_es/redshift/latest/dg/r_COPY.html (Redshift)
    """
    assert_table_lineage_equal(
        "COPY tab1 FROM 's3://mybucket/mypath'",
        {Path("s3://mybucket/mypath")},
        {"tab1"},
        dialect=dialect,
    )


@pytest.mark.parametrize(
    "dialect, path",
    [
        ("snowflake", "s3://mybucket/mypath"),
        ("tsql", "https://myaccount.blob.core.windows.net/myblobcontainer/folder1/"),
    ],
)
def test_copy_into_path(dialect: str, path: str):
    """
    check following link for syntax reference:
        Snowflake: https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html
        Microsoft T-SQL: https://docs.microsoft.com/en-us/sql/t-sql/statements/copy-into-transact-sql?view=azure-sqldw-latest
    """  # noqa: E501
    assert_table_lineage_equal(
        f"COPY INTO tab1 FROM '{path}'",
        {Path(path)},
        {"tab1"},
        dialect=dialect,
    )


@pytest.mark.parametrize("data_source", ["parquet", "json", "csv"])
@pytest.mark.parametrize("dialect", ["databricks", "sparksql"])
def test_select_from_files(data_source: str, dialect: str):
    """
    check following link for syntax reference:
        https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#run-sql-on-files-directly
    """
    assert_table_lineage_equal(
        f"SELECT * FROM {data_source}.`examples/src/main/resources/`",
        {Path("examples/src/main/resources/")},
        dialect=dialect,
    )


@pytest.mark.parametrize("dialect", ["databricks", "hive", "sparksql"])
def test_insert_overwrite_directory(dialect: str):
    """
    check following link for syntax reference:
        https://spark.apache.org/docs/latest/sql-ref-syntax-dml-insert-overwrite-directory.html
    """
    assert_table_lineage_equal(
        """INSERT OVERWRITE DIRECTORY 'hdfs://path/to/folder'
SELECT * FROM tab1""",
        {"tab1"},
        {Path("hdfs://path/to/folder")},
        dialect=dialect,
    )
