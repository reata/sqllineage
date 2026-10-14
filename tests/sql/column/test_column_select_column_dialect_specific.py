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
