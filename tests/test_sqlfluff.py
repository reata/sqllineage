from sqllineage.utils.entities import ColumnQualifierTuple
from .helpers import assert_column_lineage_equal, assert_table_lineage_equal


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


def test_insert_into_qualified_table_with_parenthesized_query():
    """
    For sqlparse, it will work if:
        1) table in unqualified
    OR  2) query is not surrounded by parenthesis
    With both in the game, it breaks.
    """
    sql = """INSERT INTO default.tab2
    (SELECT *
    FROM tab1)"""
    assert_table_lineage_equal(sql, {"tab1"}, {"default.tab2"}, test_sqlparse=False)
