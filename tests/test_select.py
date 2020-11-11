from .helpers import helper


def test_select():
    helper("SELECT col1 FROM tab1", {"tab1"})


def test_select_with_schema():
    helper("SELECT col1 FROM schema1.tab1", {"schema1.tab1"})


def test_select_with_table_name_in_backtick():
    helper("SELECT * FROM `tab1`", {"tab1"})


def test_select_with_schema_in_backtick():
    helper("SELECT col1 FROM `schema1`.`tab1`", {"schema1.tab1"})


def test_select_multi_line():
    helper(
        """SELECT col1 FROM
tab1""",
        {"tab1"},
    )


def test_select_asterisk():
    helper("SELECT * FROM tab1", {"tab1"})


def test_select_value():
    helper("SELECT 1")


def test_select_function():
    helper("SELECT NOW()")


def test_select_with_where():
    helper("SELECT * FROM tab1 WHERE col1 > val1 AND col2 = 'val2'", {"tab1"})


def test_select_with_comment():
    helper("SELECT -- comment1\n col1 FROM tab1", {"tab1"})


def test_select_with_comment_after_from():
    helper("SELECT col1\nFROM  -- comment\ntab1", {"tab1"})


def test_select_with_comment_after_join():
    helper(
        "select * from tab1 join --comment\ntab2 on tab1.x = tab2.x", {"tab1", "tab2"}
    )


def test_select_keyword_as_column_alias():
    # here `as` is the column alias
    helper("SELECT 1 `as` FROM tab1", {"tab1"})
    # the following is hive specific, MySQL doesn't allow this syntax. As of now, we don't test against it
    # helper("SELECT 1 as FROM tab1", {"tab1"})


def test_select_with_table_alias():
    helper("SELECT 1 FROM tab1 AS alias1", {"tab1"})


def test_select_count():
    helper("SELECT COUNT(*) FROM tab1", {"tab1"})


def test_select_subquery():
    helper("SELECT col1 FROM (SELECT col1 FROM tab1) dt", {"tab1"})


def test_select_subquery_without_alias():
    """this syntax is valid in SparkSQL, not for MySQL"""
    helper("SELECT col1 FROM (SELECT col1 FROM tab1)", {"tab1"})


def test_select_inner_join():
    helper("SELECT * FROM tab1 INNER JOIN tab2", {"tab1", "tab2"})


def test_select_join():
    helper("SELECT * FROM tab1 JOIN tab2", {"tab1", "tab2"})


def test_select_left_join():
    helper("SELECT * FROM tab1 LEFT JOIN tab2", {"tab1", "tab2"})


def test_select_left_join_with_extra_space_in_middle():
    helper("SELECT * FROM tab1 LEFT  JOIN tab2", {"tab1", "tab2"})


def test_select_left_semi_join():
    helper("SELECT * FROM tab1 LEFT SEMI JOIN tab2", {"tab1", "tab2"})


def test_select_left_semi_join_with_on():
    helper(
        "SELECT * FROM tab1 LEFT SEMI JOIN tab2 ON (tab1.col1 = tab2.col2)",
        {"tab1", "tab2"},
    )


def test_select_right_join():
    helper("SELECT * FROM tab1 RIGHT JOIN tab2", {"tab1", "tab2"})


def test_select_full_outer_join():
    helper("SELECT * FROM tab1 FULL OUTER JOIN tab2", {"tab1", "tab2"})


def test_select_full_outer_join_with_full_as_alias():
    helper("SELECT * FROM tab1 AS full FULL OUTER JOIN tab2", {"tab1", "tab2"})


def test_select_cross_join():
    helper("SELECT * FROM tab1 CROSS JOIN tab2", {"tab1", "tab2"})


def test_select_cross_join_with_on():
    helper(
        "SELECT * FROM tab1 CROSS JOIN tab2 on tab1.col1 = tab2.col2", {"tab1", "tab2"}
    )


def test_select_join_with_subquery():
    helper(
        "SELECT col1 FROM tab1 AS a LEFT JOIN tab2 AS b ON a.id=b.tab1_id "
        "WHERE col1 = (SELECT col1 FROM tab2 WHERE id = 1)",
        {"tab1", "tab2"},
    )


def test_select_join_in_ansi89_syntax():
    helper("SELECT * FROM tab1 a, tab2 b", {"tab1", "tab2"})


def test_with_select():
    helper("WITH tab1 AS (SELECT 1) SELECT * FROM tab1")
