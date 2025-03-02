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
