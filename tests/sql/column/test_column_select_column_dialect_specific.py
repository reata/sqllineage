import pytest

from sqllineage.utils.entities import ColumnQualifierTuple

from ...helpers import assert_column_lineage_equal


@pytest.mark.parametrize("dialect", ["tsql"])
def test_tsql_assignment_operator(dialect: str):
    """
    Assignment Operator is a Transact-SQL specific feature, used interchangeably with column alias
    https://learn.microsoft.com/en-us/sql/t-sql/language-elements/assignment-operator-transact-sql?view=sql-server-ver15
    """
    sql = """INSERT INTO foo
SELECT FirstColumnHeading = 'xyz',
       SecondColumnHeading = ProductID
FROM Production.Product"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("ProductID", "Production.Product"),
                ColumnQualifierTuple("SecondColumnHeading", "foo"),
            )
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", ["teradata"])
def test_teradata_title_phrase(dialect: str):
    """
    The TITLE phrase of a CREATE TABLE, ALTER TABLE, or SELECT statement gives a name to a column heading.
    TITLE is a Teradata extension to the ANSI SQL:2011 standard.
    It is used for display formatting and should be ignored for lineage purposes.
    https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Data-Types-and-Literals/Data-Type-Formats-and-Format-Phrases/TITLE/Examples/Example-Using-the-TITLE-Phrase-in-a-SELECT-Statement
    """
    sql = """CREATE VIEW foo AS
SELECT Name, DOB (TITLE 'Birthdate')
FROM Employee;"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("Name", "Employee"),
                ColumnQualifierTuple("Name", "foo"),
            ),
            (
                ColumnQualifierTuple("DOB", "Employee"),
                ColumnQualifierTuple("DOB", "foo"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", ["tsql"])
def test_tsql_subquery_with_column_alias_list(dialect: str):
    """
    Column alias list for derived table is not a tsql specific language feature, but since sqlfluff 4.4.0 it's parsed
    differently for tsql only: the bracketed column alias list is wrapped in an alias_column_list segment so that
    layout rules can target it, while other dialects keep it as a plain bracketed segment under alias_expression.
    https://github.com/sqlfluff/sqlfluff/issues/6733
    """
    sql = """INSERT INTO tab1
SELECT dt.col3, dt.col4
FROM (SELECT col1, col2 FROM tab2) AS dt (col3, col4)"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("col3", "tab1"),
            ),
            (
                ColumnQualifierTuple("col2", "tab2"),
                ColumnQualifierTuple("col4", "tab1"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize(
    "dialect",
    [
        "athena",
        "bigquery",
        "flink",
        "greenplum",
        "materialize",
        "postgres",
        "trino",
        "vertica",
    ],
)
def test_select_column_in_typed_array_constructor(dialect: str):
    sql = """INSERT INTO tab1
SELECT ARRAY[col1, col2 + 1, upper(col3)] AS col4
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("col4", "tab1"),
            ),
            (
                ColumnQualifierTuple("col2", "tab2"),
                ColumnQualifierTuple("col4", "tab1"),
            ),
            (
                ColumnQualifierTuple("col3", "tab2"),
                ColumnQualifierTuple("col4", "tab1"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize(
    "dialect", ["bigquery", "clickhouse", "duckdb", "snowflake", "starrocks"]
)
def test_select_column_in_array_literal(dialect: str):
    sql = """INSERT INTO tab1
SELECT [col1, col2] AS col3
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("col3", "tab1"),
            ),
            (
                ColumnQualifierTuple("col2", "tab2"),
                ColumnQualifierTuple("col3", "tab1"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", ["bigquery", "sparksql"])
def test_select_column_in_struct_constructor(dialect: str):
    sql = """INSERT INTO tab1
SELECT STRUCT(col1 AS a, col2 + 1 AS b, upper(col3) AS c) AS col4
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("col4", "tab1"),
            ),
            (
                ColumnQualifierTuple("col2", "tab2"),
                ColumnQualifierTuple("col4", "tab1"),
            ),
            (
                ColumnQualifierTuple("col3", "tab2"),
                ColumnQualifierTuple("col4", "tab1"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", ["bigquery"])
def test_select_column_in_typed_struct_constructor(dialect: str):
    sql = """INSERT INTO tab1
SELECT STRUCT<a INT64, b STRING>(col1, col2) AS col3
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("col3", "tab1"),
            ),
            (
                ColumnQualifierTuple("col2", "tab2"),
                ColumnQualifierTuple("col3", "tab1"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", ["duckdb", "snowflake"])
def test_select_column_in_object_literal(dialect: str):
    sql = """INSERT INTO tab1
SELECT {'a': col1, 'b': col2 + 1} AS col3
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("col3", "tab1"),
            ),
            (
                ColumnQualifierTuple("col2", "tab2"),
                ColumnQualifierTuple("col3", "tab1"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize(
    "dialect, operator",
    [
        ("bigquery", "=>"),
        ("databricks", "=>"),
        ("duckdb", ":="),
        ("postgres", "=>"),
        ("postgres", ":="),
    ],
)
def test_select_column_in_named_function_argument(dialect: str, operator: str):
    # the argument names a and b are not source columns
    sql = f"""INSERT INTO tab1
SELECT func(a {operator} col1, b {operator} col2 + 1) AS col3
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("col3", "tab1"),
            ),
            (
                ColumnQualifierTuple("col2", "tab2"),
                ColumnQualifierTuple("col3", "tab1"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )
