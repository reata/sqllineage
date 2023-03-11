from sqllineage.utils.entities import ColumnQualifierTuple
from .helpers import assert_column_lineage_equal


def test_current_timestamp():
    """
    current_timestamp is a keyword since ANSI SQL-2016
    sqlparse cannot produce the correct AST for this case
    """
    sql = """INSERT INTO tab1
SELECT current_timestamp as col1,
       col2,
       col3
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col2", "tab2"),
                ColumnQualifierTuple("col2", "tab1"),
            ),
            (
                ColumnQualifierTuple("col3", "tab2"),
                ColumnQualifierTuple("col3", "tab1"),
            ),
        ],
        test_sqlparse=False,
    )


def test_tsql_assignment_operator():
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
        test_sqlparse=False,
        dialect="tsql",
    )
