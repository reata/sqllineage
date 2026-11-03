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


@pytest.mark.parametrize("dialect", ["redshift"])
def test_redshift_unloading_data_to_s3(dialect: str):
    """
    https://docs.aws.amazon.com/redshift/latest/dg/t_Unloading_tables.html
    """
    assert_table_lineage_equal(
        """unload ('select * from venue')   
to 's3://amzn-s3-demo-bucket/tickit/unload/venue_' 
iam_role 'arn:aws:iam::0123456789012:role/MyRedshiftRole';""",
        {"venue"},
        {Path("s3://amzn-s3-demo-bucket/tickit/unload/venue_")},
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize(
    "stage",
    [
        "@mystage",
        "@mystage/path/",
        "@mydb.myschema.mystage/path/",
        "@~/path/",
        "@%tab2/path/",
    ],
)
def test_snowflake_select_from_stage(stage: str):
    """
    https://docs.snowflake.com/en/user-guide/querying-stage
    """
    assert_table_lineage_equal(
        f"SELECT $1, $2 FROM {stage}",
        {Path(stage)},
        dialect="snowflake",
        test_sqlparse=False,
    )
    assert_table_lineage_equal(
        f"INSERT INTO tab1 SELECT $1, $2 FROM {stage}",
        {Path(stage)},
        {"tab1"},
        dialect="snowflake",
        test_sqlparse=False,
    )


def test_snowflake_select_from_stage_with_file_format_and_alias():
    """
    https://docs.snowflake.com/en/user-guide/querying-stage
    """
    assert_table_lineage_equal(
        "SELECT t.$1, t.$2 FROM @mystage/path/ (FILE_FORMAT => 'myformat', PATTERN => '.*[.]csv') t",
        {Path("@mystage/path/")},
        dialect="snowflake",
        test_sqlparse=False,
    )


@pytest.mark.parametrize(
    "stage",
    ["@mystage/path/", "@mydb.myschema.mystage/path/"],
)
def test_snowflake_copy_into_table_from_stage(stage: str):
    """
    https://docs.snowflake.com/en/sql-reference/sql/copy-into-table
    """
    assert_table_lineage_equal(
        f"COPY INTO tab1 FROM {stage} FILE_FORMAT = (TYPE = CSV)",
        {Path(stage)},
        {"tab1"},
        dialect="snowflake",
        test_sqlparse=False,
    )


@pytest.mark.parametrize(
    "sql",
    [
        "COPY INTO tab1 FROM (SELECT $1, $2 FROM @mystage/path/) FILE_FORMAT = (TYPE = CSV)",
        "COPY INTO tab1 (col1, col2) FROM (SELECT $1, $2 FROM @mystage/path/)",
        "COPY INTO tab1 FROM (SELECT t.$1, t.$2 FROM @mystage/path/ t)",
        "COPY INTO tab1 FROM (SELECT $1, TO_DATE($2) FROM @mystage/path/ (FILE_FORMAT => 'myformat') t)",
    ],
)
def test_snowflake_copy_into_table_from_stage_with_transformation(sql: str):
    """
    https://docs.snowflake.com/en/user-guide/data-load-transform
    """
    assert_table_lineage_equal(
        sql,
        {Path("@mystage/path/")},
        {"tab1"},
        dialect="snowflake",
        test_sqlparse=False,
    )


@pytest.mark.parametrize(
    "location, path",
    [
        ("@mystage/unload/", "@mystage/unload/"),
        ("'s3://mybucket/unload/'", "s3://mybucket/unload/"),
    ],
)
@pytest.mark.parametrize(
    "source",
    [
        "tab1",
        "(SELECT col1, col2 FROM tab1)",
        "(SELECT col1, col2 FROM tab1 WHERE col1 > 0)",
    ],
)
def test_snowflake_copy_into_location(location: str, path: str, source: str):
    """
    https://docs.snowflake.com/en/sql-reference/sql/copy-into-location
    """
    assert_table_lineage_equal(
        f"COPY INTO {location} FROM {source} FILE_FORMAT = (TYPE = PARQUET) HEADER = TRUE",
        {"tab1"},
        {Path(path)},
        dialect="snowflake",
        test_sqlparse=False,
    )
