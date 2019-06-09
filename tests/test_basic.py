from sqllineage import LineageParser


def helper(sql, source_tables=None, target_tables=None):
    lp = LineageParser(sql)
    assert lp.source_tables == (source_tables or set())
    assert lp.target_tables == (target_tables or set())


def test_hello_world():
    helper("select col1 from tab1 as a left join tab2 as b on a.id=b.tab1_id "
           "where col1 = (select col1 from tab2 where id = 1)", {"tab1", "tab2"})
