from sqllineage.core import LineageParser


def helper(sql, source_tables=None, target_tables=None):
    lp = LineageParser(sql)
    assert lp.source_tables == (source_tables or set())
    assert lp.target_tables == (target_tables or set())
