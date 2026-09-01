import pytest

from sqllineage.config import SQLLineageConfig
from sqllineage.core.graph import get_graph_operator_class
from sqllineage.core.graph.networkx import NetworkXGraphOperator
from sqllineage.core.graph.rustworkx import RustworkXGraphOperator

parametrize_graph_operator_class = pytest.mark.parametrize(
    "graph_operator_class",
    [NetworkXGraphOperator, RustworkXGraphOperator],
    ids=["NetworkXGraphOperator", "RustworkXGraphOperator"],
)


def test_graph_operator_dummy():
    with pytest.raises(TypeError):
        NetworkXGraphOperator().merge(RustworkXGraphOperator())
    with pytest.raises(TypeError):
        RustworkXGraphOperator().merge(NetworkXGraphOperator())


def test_graph_operator_import():
    assert get_graph_operator_class() == NetworkXGraphOperator
    with SQLLineageConfig(
        GRAPH_OPERATOR_CLASS="sqllineage.core.graph.rustworkx.RustworkXGraphOperator"
    ):
        assert get_graph_operator_class() == RustworkXGraphOperator


def test_graph_operator_import_fail():
    with SQLLineageConfig(GRAPH_OPERATOR_CLASS="invalid_format"):
        assert get_graph_operator_class() == NetworkXGraphOperator
    with SQLLineageConfig(
        GRAPH_OPERATOR_CLASS="sqllineage.core.graph.invalid_module.UnknownGraphOperator"
    ):
        assert get_graph_operator_class() == NetworkXGraphOperator
    with SQLLineageConfig(
        GRAPH_OPERATOR_CLASS="sqllineage.core.graph.networkx.UnknownGraphOperator"
    ):
        assert get_graph_operator_class() == NetworkXGraphOperator


@parametrize_graph_operator_class
@pytest.mark.parametrize(
    "src_vertex, tgt_vertex", [("A", "ZZZ"), ("ZZZ", "A"), ("ZZZ", "ZZZ")]
)
def test_list_lineage_paths_missing_vertex_raises(
    graph_operator_class, src_vertex, tgt_vertex
):
    go = graph_operator_class()
    go.add_vertex_if_not_exist("A")
    with pytest.raises(KeyError):
        go.list_lineage_paths(src_vertex, tgt_vertex)


@parametrize_graph_operator_class
@pytest.mark.parametrize("method", ["ancestors", "descendants"])
def test_ancestors_descendants_missing_vertex_raises(graph_operator_class, method):
    go = graph_operator_class()
    go.add_vertex_if_not_exist("A")
    with pytest.raises(KeyError):
        getattr(go, method)("ZZZ")


@parametrize_graph_operator_class
def test_list_lineage_paths_same_vertex_without_self_loop_is_empty(
    graph_operator_class,
):
    go = graph_operator_class()
    go.add_vertex_if_not_exist("A")
    assert go.list_lineage_paths("A", "A") == []


@parametrize_graph_operator_class
def test_list_lineage_paths_cycle_through_seed_without_self_loop_is_empty(
    graph_operator_class,
):
    """
    A -> B -> C -> A is a cycle that revisits A, but has no A -> A edge.
    list_lineage_paths("A", "A") must report no path, not the cycle itself,
    since rustworkx's all_simple_paths (unlike networkx's) treats such a
    cycle as a valid simple path from A back to A.
    """
    go = graph_operator_class()
    for src, tgt in [("A", "B"), ("B", "C"), ("C", "A")]:
        go.add_edge_if_not_exist(src, tgt, "label")
    assert go.list_lineage_paths("A", "A") == []


@parametrize_graph_operator_class
def test_list_lineage_paths_cycle_through_seed_with_self_loop(graph_operator_class):
    """
    Same cycle as above, plus a real A -> A self-loop edge: the self-loop
    itself is the only reported path, not the longer cycle through B and C.
    """
    go = graph_operator_class()
    for src, tgt in [("A", "B"), ("B", "C"), ("C", "A"), ("A", "A")]:
        go.add_edge_if_not_exist(src, tgt, "label")
    assert go.list_lineage_paths("A", "A") == [["A", "A"]]
