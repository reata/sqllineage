from sqllineage.core import LineageParser


def helper(sql, source_tables=None, target_tables=None):
    lp = LineageParser(sql)
    assert lp.source_tables == (source_tables or set())
    assert lp.target_tables == (target_tables or set())


def test_select():
    helper("SELECT col1 FROM tab1", {"tab1"})


def test_select_join_with_subquery():
    helper("SELECT col1 FROM tab1 AS a LEFT JOIN tab2 AS b ON a.id=b.tab1_id "
           "WHERE col1 = (SELECT col1 FROM tab2 WHERE id = 1)", {"tab1", "tab2"})


def test_insert_into():
    helper("INSERT INTO tab1 VALUES (1, 2)", set(), {"tab1"})


def test_insert_into_with_keyword_table():
    helper("INSERT INTO TABLE tab1 VALUES (1, 2)", set(), {"tab1"})


def test_insert_into_with_columns():
    helper("INSERT INTO tab1 (col1, col2) SELECT * FROM tab2;", {"tab2"}, {"tab1"})


def test_insert_overwrite():
    helper("INSERT OVERWRITE table tab1 SELECT col1 FROM tab2", {"tab2"}, {"tab1"})


def test_split_statements():
    sql = "SELECT * FROM tab1; SELECT * FROM tab2;"
    assert len(LineageParser(sql).statements) == 2
