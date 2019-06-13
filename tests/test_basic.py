from sqllineage import LineageParser


def helper(sql, source_tables=None, target_tables=None):
    lp = LineageParser(sql)
    assert lp.source_tables == (source_tables or set())
    assert lp.target_tables == (target_tables or set())


def test_select():
    helper("select col1 from tab1", {"tab1"})


def test_select_join_with_subquery():
    helper("select col1 from tab1 as a left join tab2 as b on a.id=b.tab1_id "
           "where col1 = (select col1 from tab2 where id = 1)", {"tab1", "tab2"})


def test_insert_into():
    helper("insert into table tab1 values (1, 2)", set(), {"tab1"})


def test_insert_overwrite():
    helper("insert overwrite table tab1 select col1 from tab2", {"tab2"}, {"tab1"})


def test_split_statements():
    sql = "select * from tab1; select * from tab2;"
    assert len(LineageParser(sql).statements) == 2
