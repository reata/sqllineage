from sqllineage.core.models import Column, Table
from sqllineage.runner import LineageRunner


def assert_table_lineage_equal(sql, source_tables=None, target_tables=None):
    lr = LineageRunner(sql)
    for (_type, actual, expected) in zip(
        ["Source", "Target"],
        [lr.source_tables, lr.target_tables],
        [source_tables, target_tables],
    ):
        actual = set(actual)
        expected = set() if expected is None else {Table(t) for t in expected}
        assert (
            actual == expected
        ), f"\n\tExpected {_type} Table: {expected}\n\tActual {_type} Table: {actual}"


def assert_column_lineage_equal(sql, column_lineages=None):
    expected = set()
    if column_lineages:
        for src, tgt in column_lineages:
            src_col = Column(src[1])
            if src[0] is not None:
                src_col.parent = Table(src[0])
            tgt_col = Column(tgt[1])
            tgt_col.parent = Table(tgt[0])
            expected.add((src_col, tgt_col))
    lr = LineageRunner(sql)
    actual = {(lineage[0], lineage[-1]) for lineage in set(lr.get_column_lineage())}
    assert (
        set(actual) == expected
    ), f"\n\tExpected Lineage: {expected}\n\tActual Lineage: {actual}"
