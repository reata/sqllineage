from .helpers import assert_table_lineage_equal


def test_select():
    assert_table_lineage_equal("SELECT col1 FROM tab1", {"tab1"})


def test_select_with_schema():
    assert_table_lineage_equal("SELECT col1 FROM schema1.tab1", {"schema1.tab1"})


def test_select_with_schema_and_database():
    assert_table_lineage_equal(
        "SELECT col1 FROM db1.schema1.tbl1", {"db1.schema1.tbl1"}
    )


def test_select_with_table_name_in_backtick():
    assert_table_lineage_equal("SELECT * FROM `tab1`", {"tab1"})


def test_select_with_schema_in_backtick():
    assert_table_lineage_equal("SELECT col1 FROM `schema1`.`tab1`", {"schema1.tab1"})


def test_select_multi_line():
    assert_table_lineage_equal(
        """SELECT col1 FROM
tab1""",
        {"tab1"},
    )


def test_select_asterisk():
    assert_table_lineage_equal("SELECT * FROM tab1", {"tab1"})


def test_select_value():
    assert_table_lineage_equal("SELECT 1")


def test_select_function():
    assert_table_lineage_equal("SELECT NOW()")


def test_select_trim_function_with_from_keyword():
    assert_table_lineage_equal("SELECT trim(BOTH '  ' FROM '  abc  ')")


def test_select_trim_function_with_from_keyword_from_source_table():
    assert_table_lineage_equal("SELECT trim(BOTH '  ' FROM col1) FROM tab1", {"tab1"})


def test_select_with_where():
    assert_table_lineage_equal(
        "SELECT * FROM tab1 WHERE col1 > val1 AND col2 = 'val2'", {"tab1"}
    )


def test_select_with_comment():
    assert_table_lineage_equal("SELECT -- comment1\n col1 FROM tab1", {"tab1"})


def test_select_with_comment_after_from():
    assert_table_lineage_equal("SELECT col1\nFROM  -- comment\ntab1", {"tab1"})


def test_select_with_comment_after_join():
    assert_table_lineage_equal(
        "select * from tab1 join --comment\ntab2 on tab1.x = tab2.x", {"tab1", "tab2"}
    )


def test_select_keyword_as_column_alias():
    # here `as` is the column alias
    assert_table_lineage_equal("SELECT 1 `as` FROM tab1", {"tab1"})
    # the following is hive specific, MySQL doesn't allow this syntax. As of now, we don't test against it
    # helper("SELECT 1 as FROM tab1", {"tab1"})


def test_select_with_table_alias():
    assert_table_lineage_equal("SELECT 1 FROM tab1 AS alias1", {"tab1"})


def test_select_count():
    assert_table_lineage_equal("SELECT COUNT(*) FROM tab1", {"tab1"})


def test_select_subquery():
    assert_table_lineage_equal("SELECT col1 FROM (SELECT col1 FROM tab1) dt", {"tab1"})


def test_select_subquery_without_alias():
    """this syntax is valid in SparkSQL, not for MySQL"""
    assert_table_lineage_equal("SELECT col1 FROM (SELECT col1 FROM tab1)", {"tab1"})


def test_select_inner_join():
    assert_table_lineage_equal("SELECT * FROM tab1 INNER JOIN tab2", {"tab1", "tab2"})


def test_select_join():
    assert_table_lineage_equal("SELECT * FROM tab1 JOIN tab2", {"tab1", "tab2"})


def test_select_left_join():
    assert_table_lineage_equal("SELECT * FROM tab1 LEFT JOIN tab2", {"tab1", "tab2"})


def test_select_left_join_with_extra_space_in_middle():
    assert_table_lineage_equal("SELECT * FROM tab1 LEFT  JOIN tab2", {"tab1", "tab2"})


def test_select_left_semi_join():
    assert_table_lineage_equal(
        "SELECT * FROM tab1 LEFT SEMI JOIN tab2", {"tab1", "tab2"}
    )


def test_select_left_semi_join_with_on():
    assert_table_lineage_equal(
        "SELECT * FROM tab1 LEFT SEMI JOIN tab2 ON (tab1.col1 = tab2.col2)",
        {"tab1", "tab2"},
    )


def test_select_right_join():
    assert_table_lineage_equal("SELECT * FROM tab1 RIGHT JOIN tab2", {"tab1", "tab2"})


def test_select_full_outer_join():
    assert_table_lineage_equal(
        "SELECT * FROM tab1 FULL OUTER JOIN tab2", {"tab1", "tab2"}
    )


def test_select_full_outer_join_with_full_as_alias():
    assert_table_lineage_equal(
        "SELECT * FROM tab1 AS full FULL OUTER JOIN tab2", {"tab1", "tab2"}
    )


def test_select_cross_join():
    assert_table_lineage_equal("SELECT * FROM tab1 CROSS JOIN tab2", {"tab1", "tab2"})


def test_select_cross_join_with_on():
    assert_table_lineage_equal(
        "SELECT * FROM tab1 CROSS JOIN tab2 on tab1.col1 = tab2.col2", {"tab1", "tab2"}
    )


def test_select_join_with_subquery():
    assert_table_lineage_equal(
        "SELECT col1 FROM tab1 AS a LEFT JOIN tab2 AS b ON a.id=b.tab1_id "
        "WHERE col1 = (SELECT col1 FROM tab2 WHERE id = 1)",
        {"tab1", "tab2"},
    )


def test_select_join_in_ansi89_syntax():
    assert_table_lineage_equal("SELECT * FROM tab1 a, tab2 b", {"tab1", "tab2"})


def test_select_join_in_ansi89_syntax_with_subquery():
    assert_table_lineage_equal(
        "SELECT * FROM (SELECT * FROM tab1) a, (SELECT * FROM tab2) b", {"tab1", "tab2"}
    )


def test_with_select():
    assert_table_lineage_equal("WITH tab1 AS (SELECT 1) SELECT * FROM tab1")


def test_with_select_one():
    assert_table_lineage_equal(
        "WITH wtab1 AS (SELECT * FROM schema1.tab1) SELECT * FROM wtab1",
        {"schema1.tab1"},
    )


def test_with_select_many():
    assert_table_lineage_equal(
        "WITH "
        "  cte1 AS (SELECT a, b FROM table1), "
        "  cte2 AS (SELECT c, d FROM table2) "
        "SELECT b, d FROM cte1 JOIN cte2 "
        "WHERE cte1.a = cte2.c",
        {"table1", "table2"},
    )
