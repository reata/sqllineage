from sqllineage.utils.entities import ColumnQualifierTuple
from .helpers import assert_column_lineage_equal


def test_view_with_subquery_custom_columns():
    # select as subquery
    sql = "create view my_view (random1,random2) as (select col1,col2 from table)"
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "table"),
                ColumnQualifierTuple("random1", "my_view"),
            ),
            (
                ColumnQualifierTuple("col2", "table"),
                ColumnQualifierTuple("random2", "my_view"),
            ),
        ],
        test_sqlparse=False,
    )

    sql = "create view my_view (random1,random2) as select col1,col2 from table"
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "table"),
                ColumnQualifierTuple("random1", "my_view"),
            ),
            (
                ColumnQualifierTuple("col2", "table"),
                ColumnQualifierTuple("random2", "my_view"),
            ),
        ],
        test_sqlparse=False,
    )


def test_create_view_with_same_columns():
    sql = "create view new_view (col1,col2) as (select col1,col2 from table)"
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "table"),
                ColumnQualifierTuple("col1", "new_view"),
            ),
            (
                ColumnQualifierTuple("col2", "table"),
                ColumnQualifierTuple("col2", "new_view"),
            ),
        ],
        test_sqlparse=False,
    )
