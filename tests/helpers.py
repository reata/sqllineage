from sqllineage.core import LineageParser
from sqllineage.models import Table


def helper(sql, source_tables=None, target_tables=None):
    lp = LineageParser(sql)
    assert lp.source_tables == (
        set() if source_tables is None else {Table(t) for t in source_tables}
    )
    assert lp.target_tables == (
        set() if target_tables is None else {Table(t) for t in target_tables}
    )
