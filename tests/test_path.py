import pytest

from sqllineage.core.models import Path
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
    )


@pytest.mark.parametrize("data_source", ["parquet", "json", "csv"])
def test_select_from_files(data_source):
    """
    check following link for syntax reference:
        https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#run-sql-on-files-directly
    """
    assert_table_lineage_equal(
        f"SELECT * FROM {data_source}.`examples/src/main/resources/`",
        {Path("examples/src/main/resources/")},
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
    )


def test_create_view_with_single_openrowset_file():
    """
    Check for issue #290 (OPENROWSET in Synapse-on demand): https://github.com/reata/sqllineage/issues/290
    Openrowset is used in Synapse to define a file on Azure Datalake that the view should be read from on query.
    Below example has a single file to be used as the source table. Column definitions are read in at execution.
    """
    assert_table_lineage_equal(
        sql="""
        CREATE OR ALTER VIEW tbl1 AS SELECT * FROM oPENROWSET(
            BULK 'http://storage_account.dfs.core.windows.net/container/folder/subfolder/file.parquet',
            FORMAT = 'Parquet') AS t;
        """,
        source_tables={
            Path(
                "http://storage_account.dfs.core.windows.net/container/folder/subfolder/file.parquet"
            )
        },
        target_tables={"tbl1"},
    )


def test_create_view_with_single_openrowset_file_with_defined_parquet_columns():
    """
    Check for issue #290 (OPENROWSET in Synapse-on demand): https://github.com/reata/sqllineage/issues/290
    Below example has a single CSV file to be used as the source table, with column names defined.
    """
    assert_table_lineage_equal(
        sql="""
        CREATE OR ALTER VIEW tbl1 AS SELECT * FROM oPENROWSET (
            BULK 'http://storage_account.dfs.core.windows.net/container/folder/subfolder/file.parquet',
            FORMAT = 'Parquet') WITH ([filepath] VARCHAR(255), [last_calibration_time] bigint) AS t;
        """,
        source_tables={
            Path(
                "http://storage_account.dfs.core.windows.net/container/folder/subfolder/file.parquet"
            )
        },
        target_tables={"tbl1"},
    )


def test_create_view_with_single_openrowset_file_with_defined_csv_columns():
    """
    Check for issue #290 (OPENROWSET in Synapse-on demand): https://github.com/reata/sqllineage/issues/290
    Below example has a single CSV file to be used as the source table, with column names defined.
    """
    assert_table_lineage_equal(
        sql="""
        CREATE OR ALTER VIEW tbl1 AS
        SELECT  *  FROM
            OPENROWSET(
                BULK  'http://storage_account.dfs.core.windows.net/container/folder/subfolder/file.csv'
                , FORMAT = 'CSV'
        -- Contents of the file columns are defined in the below:
        ) WITH (
            [filepath] VARCHAR(255)
            ,[last_calibration_time] VARCHAR(255)
            ,[calibration_start_time] VARCHAR(255)
            ,[calibration_end_time] VARCHAR(255)
        ) AS t
        ;""",
        source_tables={
            Path(
                "http://storage_account.dfs.core.windows.net/container/folder/subfolder/file.csv"
            )
        },
        target_tables={"tbl1"},
    )


def test_create_view_with_single_openrowset_file_in_parenthesis():
    """
    Check for issue #290 (OPENROWSET in Synapse-on demand): https://github.com/reata/sqllineage/issues/290
    Below example has a single folder to be used as the source table, via a recursive scan.
    The BULK statement can optionally be in parenthesies, supporting one or many file path references.
    """
    assert_table_lineage_equal(
        sql="""
        CREATE OR ALTER VIEW tbl1 AS
        SELECT  *  FROM
            oPenRowSet( -- Test mIxEd case for functions
                bUlK ( 'http://storage_account.dfs.core.windows.net/container/folder/subfolder/**' )
                , FoRmAt = 'Parquet'
        -- Contents of the file columns are defined in the below:
        ) WITH (
            [filepath] VARCHAR(255)
            ,[last_calibration_time] VARCHAR(255)
            ,[calibration_start_time] VARCHAR(255)
            ,[calibration_end_time] VARCHAR(255)
        ) AS t
        ;""",
        source_tables={
            Path(
                "http://storage_account.dfs.core.windows.net/container/folder/subfolder/**"
            )
        },
        target_tables={"tbl1"},
    )


def test_create_view_with_multiple_openrowset():
    """
    Check for issue #290 (OPENROWSET in Synapse-on demand): https://github.com/reata/sqllineage/issues/290
    Below example uses both a singlefile and a path for Openrowset. Synapse will 'union' the discovered files together.
    """
    assert_table_lineage_equal(
        sql="""
        CREATE OR ALTER VIEW tbl1 AS
        SELECT  *  FROM
        OPENROWSET (
        BULK ( 'http://storage_account.dfs.core.windows.net/container/folder/subfolder1/**'
        , 'http://storage_account.dfs.core.windows.net/container/folder/subfolder2/subsub/single_file.parquet' )
        , FORMAT = 'Parquet') AS t
        ;""",
        source_tables={
            Path(
                "http://storage_account.dfs.core.windows.net/container/folder/subfolder1/**"
            ),
            Path(
                "http://storage_account.dfs.core.windows.net/container/folder/subfolder2/subsub/single_file.parquet"
            ),
        },
        target_tables={"tbl1"},
    )


def test_create_view_with_multiple_openrowset_with_defined_columns():
    """
    Check for issue #290 (OPENROWSET in Synapse-on demand): https://github.com/reata/sqllineage/issues/290
    Below example uses both a singlefile and a path for Openrowset. Synapse will 'union' the discovered files together.
    """
    assert_table_lineage_equal(
        sql="""
        CREATE OR ALTER VIEW tbl1 AS
        SELECT  *  FROM
            OPENROWSET (
                FORMAT = 'Parquet',
                BULK ( 'http://storage_account.dfs.core.windows.net/container/folder/subfolder1/**'
                     , 'http://storage_account.dfs.core.windows.net/container/folder/sub2/subsub/single_file.parquet')
        -- Contents of the file columns are defined in the below:
        ) WITH (
            [filepath] VARCHAR(255)
            ,[last_calibration_time] VARCHAR(255)
            ,[calibration_start_time] VARCHAR(255)
            ,[calibration_end_time] VARCHAR(255)
        ) AS t
        ;""",
        source_tables={
            Path(
                "http://storage_account.dfs.core.windows.net/container/folder/subfolder1/**"
            ),
            Path(
                "http://storage_account.dfs.core.windows.net/container/folder/sub2/subsub/single_file.parquet"
            ),
        },
        target_tables={"tbl1"},
    )


def test_create_view_with_openrowset_using_datasource():
    """
    Check for issue #290 (OPENROWSET in Synapse-on demand): https://github.com/reata/sqllineage/issues/290
    Below Openrowset syntax uses a 'DATA_SOURCE = ' parameter to specify a predefined container for the paths.
    Additionally, multiple recursive scans are used in the file path.
    """
    assert_table_lineage_equal(
        sql="""
        CREATE OR ALTER VIEW tbl1 AS
        SELECT  *  FROM
            OPENROWSET(
                BULK ( '/container/folder_with_datasource_1/**'
                     , '/container/folder_with_datasource_2/**' )
                ,DATA_SOURCE = 'some_datasource', FORMAT = 'Parquet' ) AS t
        ;""",
        source_tables={
            Path("some_datasource/container/folder_with_datasource_1/**"),
            Path("some_datasource/container/folder_with_datasource_2/**"),
        },
        target_tables={"tbl1"},
    )


def test_create_view_with_openrowset_using_datasource_and_column_names():
    """
    Check for issue #290 (OPENROWSET in Synapse-on demand): https://github.com/reata/sqllineage/issues/290
    Like above tests, but additionally specifies column names to pull from the source file.
    """
    assert_table_lineage_equal(
        sql="""
        CREATE OR ALTER VIEW tbl1 AS
        SELECT  *  FROM
            OPENROWSET(
                BULK ( '/container/folder_with_datasource_1/**'
                     , '/container/folder_with_datasource_2/**' )
                ,DATA_SOURCE = 'some_datasource', FORMAT = 'Parquet'
        -- Contents of the file columns are defined in the below:
        ) WITH (
            [filepath] VARCHAR(255)
            ,[last_calibration_time] VARCHAR(255)
            ,[calibration_start_time] VARCHAR(255)
            ,[calibration_end_time] VARCHAR(255)
        ) AS t
        ;""",
        source_tables={
            Path("some_datasource/container/folder_with_datasource_1/**"),
            Path("some_datasource/container/folder_with_datasource_2/**"),
        },
        target_tables={"tbl1"},
    )
