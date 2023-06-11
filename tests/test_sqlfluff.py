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


# sqlparse doesn't handle keyword as identifier name very well for following cases
def test_non_reserved_keyword_as_source():
    assert_table_lineage_equal(
        "SELECT col1, col2 FROM segment", {"segment"}, test_sqlparse=False
    )


def test_non_reserved_keyword_as_target():
    assert_table_lineage_equal(
        "INSERT INTO host SELECT col1, col2 FROM segment",
        {"segment"},
        {"host"},
        test_sqlparse=False,
    )


def test_non_reserved_keyword_as_cte():
    assert_table_lineage_equal(
        "WITH summary AS (SELECT * FROM segment) INSERT INTO host SELECT * FROM summary",
        {"segment"},
        {"host"},
        test_sqlparse=False,
    )


def test_non_reserved_keyword_as_column_name():
    sql = """INSERT INTO tab1
SELECT CASE WHEN locate('.', a.col1) > 0 THEN substring_index(a.col1, '.', 3) END host
FROM tab2 a"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("host", "tab1"),
            )
        ],
        test_sqlparse=False,
    )


# For top-level query parenthesis in DML, we don't treat it as subquery.
# sqlparse has some problems identifying these subqueries.
# note the table-level lineage works, only column-level lineage breaks for sqlparse
def test_create_as_with_parenthesis_around_select_statement():
    sql = "CREATE TABLE tab1 AS (SELECT * FROM tab2)"
    assert_table_lineage_equal(sql, {"tab2"}, {"tab1"}, test_sqlparse=False)


def test_create_as_with_parenthesis_around_both():
    sql = "CREATE TABLE tab1 AS (SELECT * FROM (tab2))"
    assert_table_lineage_equal(sql, {"tab2"}, {"tab1"}, test_sqlparse=False)


# specify columns in CREATE statement, sqlparse would parse my_view as function call
def test_view_with_subquery_custom_columns():
    # select as subquery
    sql = "CREATE VIEW my_view (random1,random2) AS (SELECT col1,col2 FROM tbl)"
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tbl"),
                ColumnQualifierTuple("random1", "my_view"),
            ),
            (
                ColumnQualifierTuple("col2", "tbl"),
                ColumnQualifierTuple("random2", "my_view"),
            ),
        ],
        test_sqlparse=False,
    )
    sql = "CREATE VIEW my_view (random1,random2) AS SELECT col1,col2 FROM tbl"
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tbl"),
                ColumnQualifierTuple("random1", "my_view"),
            ),
            (
                ColumnQualifierTuple("col2", "tbl"),
                ColumnQualifierTuple("random2", "my_view"),
            ),
        ],
        test_sqlparse=False,
    )


def test_create_view_with_same_columns():
    sql = "CREATE VIEW my_view (col1,col2) AS (SELECT col1,col2 FROM tbl)"
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tbl"),
                ColumnQualifierTuple("col1", "my_view"),
            ),
            (
                ColumnQualifierTuple("col2", "tbl"),
                ColumnQualifierTuple("col2", "my_view"),
            ),
        ],
        test_sqlparse=False,
    )


# specify columns in INSERT statement, sqlparse would parse my_view as function call
def test_insert_into_with_columns():
    # table lineage works for sqlparse
    assert_table_lineage_equal(
        "INSERT INTO tab1 (col1, col2) SELECT * FROM tab2;",
        {"tab2"},
        {"tab1"},
        test_sqlparse=False,
    )


def test_insert_into_with_columns_and_select_union():
    assert_table_lineage_equal(
        "INSERT INTO tab1 (col1, col2) SELECT * FROM tab2 UNION SELECT * FROM tab3",
        {"tab2", "tab3"},
        {"tab1"},
        test_sqlparse=False,
    )
    assert_table_lineage_equal(
        "INSERT INTO tab1 (col1, col2) (SELECT * FROM tab2 UNION SELECT * FROM tab3)",
        {"tab2", "tab3"},
        {"tab1"},
        test_sqlparse=False,
    )


def test_insert_with_custom_columns():
    # test with query as subquery
    sql = "INSERT INTO tgt_tbl(random1, random2) (SELECT col1,col2 FROM src_tbl)"
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "src_tbl"),
                ColumnQualifierTuple("random1", "tgt_tbl"),
            ),
            (
                ColumnQualifierTuple("col2", "src_tbl"),
                ColumnQualifierTuple("random2", "tgt_tbl"),
            ),
        ],
        test_sqlparse=False,
    )

    # test with plain query
    sql = "INSERT INTO tgt_tbl(random1, random2) SELECT col1,col2 FROM src_tbl_new"
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "src_tbl_new"),
                ColumnQualifierTuple("random1", "tgt_tbl"),
            ),
            (
                ColumnQualifierTuple("col2", "src_tbl_new"),
                ColumnQualifierTuple("random2", "tgt_tbl"),
            ),
        ],
        test_sqlparse=False,
    )
