import networkx as nx

from sqllineage import SQLPARSE_DIALECT
from sqllineage.core.models import Column, Table
from sqllineage.runner import LineageRunner


def assert_table_lineage(lr: LineageRunner, source_tables=None, target_tables=None):
    for _type, actual, expected in zip(
        ["Source", "Target"],
        [lr.source_tables, lr.target_tables],
        [source_tables, target_tables],
    ):
        actual = set(actual)
        expected = (
            set()
            if expected is None
            else {Table(t) if isinstance(t, str) else t for t in expected}
        )
        assert (
            actual == expected
        ), f"\n\tExpected {_type} Table: {expected}\n\tActual {_type} Table: {actual}"


def assert_column_lineage(lr: LineageRunner, column_lineages=None):
    expected = set()
    if column_lineages:
        for src, tgt in column_lineages:
            src_col: Column = Column(src.column)
            if src.qualifier is not None:
                src_col.parent = Table(src.qualifier)
            tgt_col: Column = Column(tgt.column)
            tgt_col.parent = Table(tgt.qualifier)
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
    lr = LineageRunner(sql, dialect=SQLPARSE_DIALECT)
    lr_sqlfluff = LineageRunner(sql, dialect=dialect)
    if test_sqlparse:
        assert_table_lineage(lr, source_tables, target_tables)
    if test_sqlfluff:
        assert_table_lineage(lr_sqlfluff, source_tables, target_tables)
    if test_sqlparse and test_sqlfluff:
        assert_lr_graphs_match(lr, lr_sqlfluff)


def assert_column_lineage_equal(
    sql: str,
    column_lineages=None,
    dialect: str = "ansi",
    test_sqlfluff: bool = True,
    test_sqlparse: bool = True,
):
    lr = LineageRunner(sql, dialect=SQLPARSE_DIALECT)
    lr_sqlfluff = LineageRunner(sql, dialect=dialect)
    if test_sqlparse:
        assert_column_lineage(lr, column_lineages)
    if test_sqlfluff:
        assert_column_lineage(lr_sqlfluff, column_lineages)
    if test_sqlparse and test_sqlfluff:
        assert_lr_graphs_match(lr, lr_sqlfluff)


def assert_lr_graphs_match(lr: LineageRunner, lr_sqlfluff: LineageRunner) -> None:
    assert nx.is_isomorphic(lr._sql_holder.graph, lr_sqlfluff._sql_holder.graph), (
        f"\n\tGraph with sqlparse: {lr._sql_holder.graph}\n\t"
        f"Graph with sqlfluff: {lr_sqlfluff._sql_holder.graph}"
    )
