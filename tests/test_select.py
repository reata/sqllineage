from .helpers import helper


def test_select():
    helper("SELECT col1 FROM tab1", {"tab1"})


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


def test_select_keyword_as_column_alias():
    # here `as` is the column alias
    helper("SELECT 1 `as` FROM tab1", {"tab1"})
    # the following is hive specific, MySQL doesn't allow this syntax. As of now, we don't test against it
    # helper("SELECT 1 as FROM tab1", {"tab1"})


def test_select_with_table_alias():
    helper("SELECT 1 FROM tab1 AS alias1", {"tab1"})


def test_select_count():
    helper("SELECT COUNT(*) FROM tab1", {"tab1"})


def test_select_inner_join():
    helper("SELECT * FROM tab1 INNER JOIN tab2", {"tab1", "tab2"})


def test_select_join():
    helper("SELECT * FROM tab1 JOIN tab2", {"tab1", "tab2"})


def test_select_left_join():
    helper("SELECT * FROM tab1 LEFT JOIN tab2", {"tab1", "tab2"})


def test_select_right_join():
    helper("SELECT * FROM tab1 RIGHT JOIN tab2", {"tab1", "tab2"})


def test_select_full_outer_join():
    helper("SELECT * FROM tab1 FULL OUTER JOIN tab2", {"tab1", "tab2"})


def test_select_cross_join():
    helper("SELECT * FROM tab1 CROSS JOIN tab2", {"tab1", "tab2"})


def test_select_join_with_subquery():
    helper("SELECT col1 FROM tab1 AS a LEFT JOIN tab2 AS b ON a.id=b.tab1_id "
           "WHERE col1 = (SELECT col1 FROM tab2 WHERE id = 1)", {"tab1", "tab2"})
