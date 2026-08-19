import os
import tempfile

import pytest

from sqllineage.cli import main
from sqllineage.config import SQLLineageConfig
from sqllineage.core.models import Column, Path, SubQuery, Table
from sqllineage.runner import LineageRunner
from sqllineage.utils.constant import LineageLevel

from ..helpers import _gen_graph_operators, assert_table_lineage_equal

parametrize_graph_operator = pytest.mark.parametrize(
    "graph_operator", _gen_graph_operators(), ids=lambda go: go.rsplit(".", 1)[-1]
)


@parametrize_graph_operator
def test_runner_dummy(graph_operator):
    with SQLLineageConfig(GRAPH_OPERATOR_CLASS=graph_operator):
        runner = LineageRunner(
            """insert into tab2 select col1, col2, col3, col4, col5, col6 from tab1;
        insert into tab3 select * from tab2""",
            verbose=True,
        )
        assert str(runner)
        assert runner.to_cytoscape() is not None
        assert runner.to_cytoscape(level=LineageLevel.COLUMN) is not None


def test_statements_trim_comment():
    comment = "------------------\n"
    sql = "select * from dual;"
    assert LineageRunner(comment + sql).statements()[0] == sql


def test_silent_mode():
    sql = "begin; select * from dual;"
    LineageRunner(sql, dialect="greenplum", silent_mode=True)._eval()


def test_get_column_lineage_exclude_subquery_inpath():
    v_sql = "insert into ta select b from (select b from tb union all select c from tc ) sub"
    parse = LineageRunner(sql=v_sql)
    for col_tuple in parse.get_column_lineage(exclude_subquery_columns=True):
        for col in col_tuple:
            assert not isinstance(col.parent, SubQuery)


# two columns per table, so "tab2.col1" is distinct from "tab2.col2"
_MULTI_COL_SQL = "insert into tab2 select col1, col2 from tab1; insert into tab3 select col1 from tab2"
# one column per table, so a bare table name resolves to exactly one column
_SINGLE_COL_SQL = (
    "insert into tab2 select col1 from tab1; insert into tab3 select col1 from tab2"
)


def _col(name: str, table: str) -> Column:
    col = Column(name)
    col.parent = Table(table)
    return col


def _naive_filter(all_paths, node):
    """reference implementation: compute everything, keep paths touching node"""
    return {path for path in all_paths if node in path}


@parametrize_graph_operator
def test_node_filter_matches_naive_filtering(graph_operator):
    with SQLLineageConfig(GRAPH_OPERATOR_CLASS=graph_operator):
        full = set(LineageRunner(_MULTI_COL_SQL).get_column_lineage())
        for node in [
            _col("col1", "tab2"),
            _col("col2", "tab2"),
            _col("col2", "tab1"),
            _col("col1", "tab3"),
        ]:
            filtered = set(LineageRunner(_MULTI_COL_SQL).get_column_lineage(node=node))
            assert filtered == _naive_filter(full, node), node


def test_node_filter_by_column_object():
    lr = LineageRunner(_MULTI_COL_SQL)
    lr._eval()
    col = _col("col1", "tab2")
    result = lr.get_column_lineage(node=col)
    assert len(result) > 0
    assert all(col in path for path in result)


def test_node_filter_disambiguates_same_column_name_different_table():
    # col1 in tab2 and col1 in tab3 are two independent siblings both sourced
    # from tab1.col1; a Column node must resolve to exactly the requested
    # one, not conflate same-named columns from different tables
    sql = (
        "insert into tab2 select col1 from tab1; insert into tab3 select col1 from tab1"
    )
    lr = LineageRunner(sql)
    result = lr.get_column_lineage(node=_col("col1", "tab2"))
    assert result == [(_col("col1", "tab1"), _col("col1", "tab2"))]


def test_node_filter_no_match_returns_empty():
    lr = LineageRunner(_SINGLE_COL_SQL)
    assert lr.get_column_lineage(node=_col("does_not_exist", "tab2")) == []


def test_node_filter_rejects_non_column():
    lr = LineageRunner(_SINGLE_COL_SQL)
    with pytest.raises(TypeError):
        lr.get_column_lineage(node=Table("tab2"))


def test_node_filter_none_unchanged():
    lr1 = LineageRunner(_MULTI_COL_SQL)
    lr2 = LineageRunner(_MULTI_COL_SQL)
    assert lr1.get_column_lineage() == lr2.get_column_lineage(node=None)


def test_node_filter_cone_with_no_reachable_target_returns_empty():
    # sub.c is selected from tb but never projected into ta, so it has no
    # path to a real table target once subquery-ending paths are pruned
    sql = "insert into ta select b from (select b, c from tb) sub"
    lr = LineageRunner(sql)
    assert lr.get_column_lineage(node=_col("c", "tb")) == []


def test_node_filter_exclude_subquery_columns_drops_collapsed_path():
    # sub.b has no upstream column (literal value), so once the subquery
    # column is stripped, the remaining path is a single column and dropped
    sql = "insert into ta select b from (select 1 as b) sub"
    lr = LineageRunner(sql)
    assert (
        lr.get_column_lineage(node=_col("b", "ta"), exclude_subquery_columns=True) == []
    )


@pytest.mark.xfail(
    reason="cone_lineage_paths joins ancestor/descendant paths without checking "
    "vertex-disjointness; a cycle longer than a self-loop makes the shared "
    "node appear twice in one returned path (utils.py cone_lineage_paths)",
    strict=True,
)
@parametrize_graph_operator
def test_node_filter_cone_path_is_simple_across_cycle(graph_operator):
    # tabX -> tabSeed -> tabX forms a two-table cycle around the seed node;
    # every path returned for get_column_lineage(node=...) must still be a
    # simple path (no repeated column), just like the unfiltered result.
    sql = (
        "insert into tabX select col1 from tab0; "
        "insert into tabSeed select col1 from tabX; "
        "insert into tabX select col1 from tabSeed; "
        "insert into tabY select col1 from tabX"
    )
    seed = _col("col1", "tabSeed")
    with SQLLineageConfig(GRAPH_OPERATOR_CLASS=graph_operator):
        result = LineageRunner(sql).get_column_lineage(node=seed)
        for path in result:
            assert len(path) == len(set(path)), path


def test_get_column_lineage_deterministic_order_for_tied_endpoints():
    """
    Two distinct paths sharing the same source and target column, differing
    only in an intermediate column, must sort the same way regardless of the
    order they come out of the underlying set, not just by (target, source).
    """
    runner = LineageRunner("select * from dual")
    runner._eval()

    src_col = Column("src")
    src_col.parent = Table("tab1")
    tgt_col = Column("tgt")
    tgt_col.parent = Table("tab3")
    mid1 = Column("mid1")
    mid1.parent = SubQuery("select 1", "select 1", "sub1")
    mid2 = Column("mid2")
    mid2.parent = SubQuery("select 2", "select 2", "sub2")

    path_a = (src_col, mid1, tgt_col)
    path_b = (src_col, mid2, tgt_col)

    class FakeHolder:
        def __init__(self, paths):
            self._paths = paths

        def get_column_lineage(self, *args, **kwargs):
            return self._paths

    runner._sql_holder = FakeHolder([path_a, path_b])
    result_ab = runner.get_column_lineage()
    runner._sql_holder = FakeHolder([path_b, path_a])
    result_ba = runner.get_column_lineage()

    assert result_ab == result_ba == [path_a, path_b]


def test_find_nodes_by_predicate():
    lr = LineageRunner(_MULTI_COL_SQL)
    result = lr.find_nodes(lambda v: isinstance(v, Column) and v.raw_name == "col1")
    assert len(result) > 0
    assert all(isinstance(v, Column) and v.raw_name == "col1" for v in result)


def test_find_nodes_matches_tables_and_columns():
    lr = LineageRunner(_MULTI_COL_SQL)
    result = lr.find_nodes(lambda v: "tab2" in str(v))
    assert any(isinstance(v, Table) for v in result)
    assert any(isinstance(v, Column) for v in result)


def test_find_nodes_no_match_returns_empty():
    lr = LineageRunner(_MULTI_COL_SQL)
    assert lr.find_nodes(lambda v: str(v) == "does_not_exist") == []


def test_find_nodes_matches_path():
    lr = LineageRunner("COPY tab1 FROM 's3://mybucket/mypath'", dialect="postgres")
    result = lr.find_nodes(lambda v: isinstance(v, Path))
    assert result == [Path("s3://mybucket/mypath")]


@parametrize_graph_operator
def test_column_selfloop_matches_across_graph_operators(graph_operator):
    # insert into tab1 select col1 from tab1: tab1.col1 has a real self-loop
    # lineage edge; both backends must report the same shape for it, see
    # https://github.com/Qiskit/rustworkx/issues/1617 for the divergence this
    # guards against. Asserting the same fixed expected value on every
    # backend is what makes them match each other.
    sql = "insert into tab1 select col1 from tab1"
    seed = _col("col1", "tab1")
    with SQLLineageConfig(GRAPH_OPERATOR_CLASS=graph_operator):
        assert set(LineageRunner(sql).get_column_lineage()) == {(seed, seed)}


@parametrize_graph_operator
def test_column_selfloop_via_node_filter_matches_across_graph_operators(
    graph_operator,
):
    sql = "insert into tab1 select col1 from tab1"
    seed = _col("col1", "tab1")
    with SQLLineageConfig(GRAPH_OPERATOR_CLASS=graph_operator):
        result = set(LineageRunner(sql).get_column_lineage(node=seed))
        assert result == {(seed, seed)}


@parametrize_graph_operator
def test_column_selfloop_with_downstream_target(graph_operator):
    # tab1.col1 self-loops, and also feeds tab2.col1; both the self-loop path
    # and the genuine downstream path must be present, on both backends.
    sql = (
        "insert into tab1 select col1 from tab1; "
        "insert into tab2 select col1 from tab1"
    )
    seed = _col("col1", "tab1")
    down = _col("col1", "tab2")
    with SQLLineageConfig(GRAPH_OPERATOR_CLASS=graph_operator):
        result = set(LineageRunner(sql).get_column_lineage(node=seed))
        assert result == {(seed, seed), (seed, down)}


@parametrize_graph_operator
def test_table_selfloop_classified_as_source_and_target(graph_operator):
    sql = "insert into tab1 select * from tab1"
    with SQLLineageConfig(GRAPH_OPERATOR_CLASS=graph_operator):
        lr = LineageRunner(sql)
        assert Table("tab1") in lr.source_tables
        assert Table("tab1") in lr.target_tables
        assert Table("tab1") not in lr.intermediate_tables


def test_respect_sqlfluff_configuration_file():
    sqlfluff_config = """[sqlfluff:templater:jinja:context]
num_things=456
tbl_name=my_table"""
    cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tmpdirname:
        try:
            os.chdir(tmpdirname)
            with open(".sqlfluff", "w") as f:
                f.write(sqlfluff_config)
            sql = "SELECT {{ num_things }} FROM {{ tbl_name }} WHERE id > 10 LIMIT 5"
            assert_table_lineage_equal(sql, {"my_table"}, test_sqlparse=False)
        finally:
            os.chdir(cwd)


def test_respect_nested_sqlfluff_configuration_file():
    sqlfluff_config = """[sqlfluff:templater:jinja:context]
num_things=456
tbl_name=my_table"""
    cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tmpdirname:
        try:
            os.chdir(tmpdirname)
            nested_dir = os.path.join(tmpdirname, "nested_dir")
            os.mkdir(nested_dir)
            with open(os.path.join(nested_dir, ".sqlfluff"), "w") as f:
                f.write(sqlfluff_config)
            with open(os.path.join(nested_dir, "nested.sql"), "w") as f:
                f.write(
                    "SELECT {{ num_things }} FROM {{ tbl_name }} WHERE id > 10 LIMIT 5"
                )
            main(["-f", nested_dir + "/nested.sql"])
        finally:
            os.chdir(cwd)
