from sqllineage.models import Column, Table
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


def assert_column_lineage_equal(sql, column_lineages=None):
    column_lineages = (
        {
            (
                Column(lineage[0]),
                Column(lineage[1]),
            )
            for lineage in column_lineages
        }
        if column_lineages
        else set()
    )
    lr = LineageRunner(sql)
    lr._eval()
    assert len(lr._lineage_results) == 1
    lineage_result = lr._lineage_results[0]
    assert (
        lineage_result.column == column_lineages
    ), f"\n\tExpected Lineage: {column_lineages}\n\tActual Lineage: {lineage_result.column}"
