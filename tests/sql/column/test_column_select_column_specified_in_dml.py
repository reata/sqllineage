"""
specify columns in CREATE/INSERT statement.
DO NOT support this feature with non-validating dialect because sqlparse would parse table/view name as function call
"""

from sqllineage.utils.entities import ColumnQualifierTuple

from ...helpers import assert_column_lineage_equal, assert_table_lineage_equal


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
    sql = "CREATE VIEW my_view (random1,random2) AS ((SELECT col1,col2 FROM tbl))"
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
    sql = "CREATE VIEW my_view (random1,random2) AS (((SELECT col1,col2 FROM tbl)))"
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
    assert_table_lineage_equal(
        "INSERT INTO tab1 (col1, col2) ((SELECT * FROM tab2 UNION SELECT * FROM tab3))",
        {"tab2", "tab3"},
        {"tab1"},
        test_sqlparse=False,
    )
    assert_table_lineage_equal(
        "INSERT INTO tab1 (col1, col2) (((SELECT * FROM tab2 UNION SELECT * FROM tab3)))",
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


def test_insert_with_custom_columns_and_cte_within_query():
    sql = """INSERT INTO tab2 (col1)
WITH cte1 AS (SELECT col2 FROM tab1)
SELECT col2 FROM cte1"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col2", "tab1"),
                ColumnQualifierTuple("col1", "tab2"),
            )
        ],
        test_sqlparse=False,
    )
