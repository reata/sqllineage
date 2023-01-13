import networkx as nx
from sqllineage.core.models import Table, Column

from sqllineage.runner import LineageRunner
from sqllineage.sqlfluff_core.models import SqlFluffColumn, SqlFluffTable


def assert_table_lineage(
    lr: LineageRunner,
    source_tables=None,
    target_tables=None,
    test_sqlfluff: bool = False,
):
    for (_type, actual, expected) in zip(
        ["Source", "Target"],
        [lr.source_tables, lr.target_tables],
        [source_tables, target_tables],
    ):
        actual = set(actual)
        expected = (
            set()
            if expected is None
            else {
                (SqlFluffTable(t) if test_sqlfluff else Table(t))
                if isinstance(t, str)
                else t
                for t in expected
            }
        )
        assert (
            actual == expected
        ), f"\n\tExpected {_type} Table: {expected}\n\tActual {_type} Table: {actual}"


def assert_column_lineage(
    lr: LineageRunner, column_lineages=None, test_sqlfluff: bool = False
):
    expected = set()
    if column_lineages:
        for src, tgt in column_lineages:
            src_col = (
                SqlFluffColumn(src.column) if test_sqlfluff else Column(src.column)
            )
            if src.qualifier is not None:
                src_col.parent = (
                    SqlFluffTable(src.qualifier)
                    if test_sqlfluff
                    else Table(src.qualifier)
                )
            tgt_col = (
                SqlFluffColumn(tgt.column) if test_sqlfluff else Column(tgt.column)
            )
            tgt_col.parent = (
                SqlFluffTable(tgt.qualifier) if test_sqlfluff else Table(tgt.qualifier)
            )
            expected.add((src_col, tgt_col))
    actual = {(lineage[0], lineage[-1]) for lineage in set(lr.get_column_lineage())}

    assert (
        set(actual) == expected
    ), f"\n\tExpected Lineage: {expected}\n\tActual Lineage: {actual}"


def assert_table_lineage_equal(
    sql: str,
    source_tables=None,
    target_tables=None,
    dialect: str = "ansi",
    test_sqlfluff: bool = True,
    test_sqlparse: bool = True,
):
    lr = LineageRunner(sql)
    if test_sqlparse:
        assert_table_lineage(lr, source_tables, target_tables)
    else:
        lr.get_column_lineage()

    if test_sqlfluff:
        lr_sqlfluff = LineageRunner(sql, dialect=dialect, use_sqlparse=False)
        assert_table_lineage(lr_sqlfluff, source_tables, target_tables, test_sqlfluff)
        assert_lr_graphs_match(lr_sqlfluff, lr)


def assert_column_lineage_equal(
    sql: str,
    column_lineages=None,
    dialect: str = "ansi",
    test_sqlfluff: bool = True,
    tesl_sqlparse: bool = True,
):
    lr = LineageRunner(sql)
    if tesl_sqlparse:
        assert_column_lineage(lr, column_lineages)
    else:
        lr.get_column_lineage()

    if test_sqlfluff:
        lr_sqlfluff = LineageRunner(sql, dialect=dialect, use_sqlparse=False)
        assert_column_lineage(lr_sqlfluff, column_lineages, test_sqlfluff)
        assert_lr_graphs_match(lr, lr_sqlfluff)


def assert_lr_graphs_match(lr: LineageRunner, lr_sqlfluff: LineageRunner) -> None:
    assert nx.is_isomorphic(lr._sql_holder.graph, lr_sqlfluff._sql_holder.graph), (
        f"\n\tGraph with sqlparse: {lr._sql_holder.graph}\n\t"
        f"Graph with sqlfluff: {lr_sqlfluff._sql_holder.graph}"
    )
