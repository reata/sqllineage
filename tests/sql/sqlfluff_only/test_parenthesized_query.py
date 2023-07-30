"""
For top-level query parenthesis in DML, we don't treat it as subquery.
sqlparse has some problems identifying these subqueries.
note the table-level lineage works, only column-level lineage breaks for sqlparse
"""

from sqllineage.utils.entities import ColumnQualifierTuple
from ...helpers import assert_column_lineage_equal, assert_table_lineage_equal


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


def test_create_as_with_parenthesis_around_select_statement():
    sql = "CREATE TABLE tab1 AS (SELECT * FROM tab2)"
    assert_table_lineage_equal(sql, {"tab2"}, {"tab1"}, test_sqlparse=False)


def test_create_as_with_parenthesis_around_both():
    sql = "CREATE TABLE tab1 AS (SELECT * FROM (tab2))"
    assert_table_lineage_equal(sql, {"tab2"}, {"tab1"}, test_sqlparse=False)


def test_cte_inside_bracket_of_insert():
    sql = """INSERT INTO tab3 (WITH tab1 AS (SELECT * FROM tab2) SELECT * FROM tab1)"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("*", "tab2"),
                ColumnQualifierTuple("*", "tab3"),
            ),
        ],
        test_sqlparse=False,
    )
