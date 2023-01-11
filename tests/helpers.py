import networkx as nx

from sqllineage.runner import LineageRunner
from sqllineage.sqlfluff_core.models import SqlFluffColumn, SqlFluffTable


def assert_table_lineage_equal(
    sql, source_tables=None, target_tables=None, dialect: str = "ansi"
):
    lr = LineageRunner(sql, dialect=dialect)

    assert_lr_graphs_match(dialect, lr, sql)

    for (_type, actual, expected) in zip(
        ["Source", "Target"],
        [lr.source_tables, lr.target_tables],
        [source_tables, target_tables],
    ):
        actual = set(actual)
        expected = (
            set()
            if expected is None
            else {SqlFluffTable(t) if isinstance(t, str) else t for t in expected}
        )

        assert (
            actual == expected
        ), f"\n\tExpected {_type} Table: {expected}\n\tActual {_type} Table: {actual}"


def assert_column_lineage_equal(sql, column_lineages=None, dialect: str = "ansi"):
    lr = LineageRunner(sql, dialect=dialect)

    assert_lr_graphs_match(dialect, lr, sql)

    expected = set()
    if column_lineages:
        for src, tgt in column_lineages:
            src_col = SqlFluffColumn(src.column)
            if src.qualifier is not None:
                src_col.parent = SqlFluffTable(src.qualifier)
            tgt_col = SqlFluffColumn(tgt.column)
            tgt_col.parent = SqlFluffTable(tgt.qualifier)
            expected.add((src_col, tgt_col))
    actual = {(lineage[0], lineage[-1]) for lineage in set(lr.get_column_lineage())}

    assert (
        set(actual) == expected
    ), f"\n\tExpected Lineage: {expected}\n\tActual Lineage: {actual}"


def assert_lr_graphs_match(dialect: str, lr: LineageRunner, sql: str) -> None:
    # ensure old and new implementation generates same graph
    lr_sql_parse = LineageRunner(sql, dialect=dialect, use_sqlparse=True)
    lr_sql_parse.get_column_lineage()
    # force run lineage
    lr.get_column_lineage()
    assert nx.is_isomorphic(lr_sql_parse._sql_holder.graph, lr._sql_holder.graph), (
        f"\n\tOld graph with sqlparse: {lr_sql_parse._sql_holder.graph}\n\t"
        f"New graph with sqlfluff: {lr._sql_holder.graph}"
    )
