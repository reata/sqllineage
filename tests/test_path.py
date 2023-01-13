import pytest

from sqllineage.core.models import Path
from sqllineage.sqlfluff_core.models import SqlFluffPath
from .helpers import assert_table_lineage_equal


def test_copy_from_path():
    """
    check following link for syntax specs:
        Redshift: https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html
    """
    assert_table_lineage_equal(
        "COPY tab1 FROM 's3://mybucket/mypath'",
        {Path("s3://mybucket/mypath")},
        {"tab1"},
        test_sqlfluff=False,
    )
    assert_table_lineage_equal(
        "COPY tab1 FROM 's3://mybucket/mypath'",
        {SqlFluffPath("s3://mybucket/mypath")},
        {"tab1"},
        "redshift",
        test_sqlparse=False,
    )


def test_copy_into_path():
    """
    check following link for syntax reference:
        Snowflake: https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html
        Microsoft T-SQL: https://docs.microsoft.com/en-us/sql/t-sql/statements/copy-into-transact-sql?view=azure-sqldw-latest  # noqa
    """
    assert_table_lineage_equal(
        "COPY INTO tab1 FROM 's3://mybucket/mypath'",
        {Path("s3://mybucket/mypath")},
        {"tab1"},
        test_sqlfluff=False,
    )
    assert_table_lineage_equal(
        "COPY INTO tab1 FROM 's3://mybucket/mypath'",
        {SqlFluffPath("s3://mybucket/mypath")},
        {"tab1"},
        "snowflake",
        test_sqlparse=False,
    )


# deactivated for sqlfluff since it can not be parsed properly
@pytest.mark.parametrize("data_source", ["parquet", "json", "csv"])
def test_select_from_files(data_source):
    """
    check following link for syntax reference:
        https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#run-sql-on-files-directly
    """
    assert_table_lineage_equal(
        f"SELECT * FROM {data_source}.`examples/src/main/resources/`",
        {Path("examples/src/main/resources/")},
        test_sqlfluff=False,
    )


def test_insert_overwrite_directory():
    """
    check following link for syntax reference:
        https://spark.apache.org/docs/latest/sql-ref-syntax-dml-insert-overwrite-directory.html
    """
    assert_table_lineage_equal(
        """INSERT OVERWRITE DIRECTORY 'hdfs://path/to/folder'
SELECT * FROM tab1""",
        {"tab1"},
        {Path("hdfs://path/to/folder")},
        test_sqlfluff=False,
    )
    assert_table_lineage_equal(
        """INSERT OVERWRITE DIRECTORY 'hdfs://path/to/folder'
SELECT * FROM tab1""",
        {"tab1"},
        {SqlFluffPath("hdfs://path/to/folder")},
        "sparksql",
        test_sqlparse=False,
    )
