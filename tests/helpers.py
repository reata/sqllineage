from sqllineage.models import Table
from sqllineage.runner import LineageRunner


def assert_table_lineage_equal(sql, source_tables=None, target_tables=None):
    lp = LineageRunner(sql)
    for (_type, actual, expected) in zip(
        ["Source", "Target"],
        [lp.source_tables, lp.target_tables],
        [source_tables, target_tables],
    ):
        actual = set(actual)
        expected = set() if expected is None else {Table(t) for t in expected}
        assert (
            actual == expected
        ), f"\n\tExpected {_type} Table: {expected}\n\tActual {_type} Table: {actual}"
