import pytest

from sqllineage.utils.entities import ColumnQualifierTuple
from ...helpers import assert_column_lineage_equal, assert_table_lineage_equal


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


@pytest.mark.parametrize("dialect", ["snowflake", "bigquery"])
def test_create_clone(dialect: str):
    """
    Language manual:
        https://cloud.google.com/bigquery/docs/table-clones-create
        https://docs.snowflake.com/en/sql-reference/sql/create-clone
    Note clone is not a keyword in sqlparse, we'll skip testing for it.
    """
    assert_table_lineage_equal(
        "CREATE TABLE tab2 CLONE tab1",
        {"tab1"},
        {"tab2"},
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", ["snowflake"])
def test_alter_table_swap_partition(dialect: str):
    """
    See https://docs.snowflake.com/en/sql-reference/sql/alter-table for language manual
    Note swap is not a keyword in sqlparse, we'll skip testing for it.
    """
    assert_table_lineage_equal(
        "ALTER TABLE tab1 SWAP WITH tab2",
        {"tab2"},
        {"tab1"},
        dialect=dialect,
        test_sqlparse=False,
    )
