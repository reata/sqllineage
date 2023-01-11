from sqllineage.core.models import Table
from sqllineage.sqlfluff_core.models import SqlFluffColumn
from sqllineage.runner import LineageRunner
import networkx as nx


def assert_table_lineage_equal(
    sql, source_tables=None, target_tables=None, dialect: str = "ansi"
):
    lr = LineageRunner(sql, dialect=dialect)
    for (_type, actual, expected) in zip(
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

        # ensure old and new implementation generates same graph
        lr_sql_parse = LineageRunner(sql, dialect=dialect, use_sqlparse=True)
        lr_sql_parse.get_column_lineage()

        # ensure old and new implementation generates same graph
        assert (
            nx.is_isomorphic(lr_sql_parse._sql_holder.graph, lr._sql_holder.graph)
        ), f"\n\tOld graph with sqlparse: {lr_sql_parse._sql_holder.graph}\n\tNew graph with sqlfluff: {lr._sql_holder.graph}"

        assert (
            actual == expected
        ), f"\n\tExpected {_type} Table: {expected}\n\tActual {_type} Table: {actual}"


def assert_column_lineage_equal(sql, column_lineages=None, dialect: str = "ansi"):
    expected = set()
    if column_lineages:
        for src, tgt in column_lineages:
            src_col = SqlFluffColumn(src.column)
            if src.qualifier is not None:
                src_col.parent = Table(src.qualifier)
            tgt_col = SqlFluffColumn(tgt.column)
            tgt_col.parent = Table(tgt.qualifier)
            expected.add((src_col, tgt_col))
    lr = LineageRunner(sql, dialect=dialect)
    actual = {(lineage[0], lineage[-1]) for lineage in set(lr.get_column_lineage())}

    lr_sql_parse = LineageRunner(sql, dialect=dialect, use_sqlparse=True)
    lr_sql_parse.get_column_lineage()

    # ensure old and new implementation generates same graph
    assert (
        nx.is_isomorphic(lr_sql_parse._sql_holder.graph, lr._sql_holder.graph)
    ), f"\n\tOld graph with sqlparse: {lr_sql_parse._sql_holder.graph}\n\tNew graph with sqlfluff: {lr._sql_holder.graph}"

    assert (
        set(actual) == expected
    ), f"\n\tExpected Lineage: {expected}\n\tActual Lineage: {actual}"
