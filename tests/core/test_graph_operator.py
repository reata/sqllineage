import pytest

from sqllineage.config import SQLLineageConfig
from sqllineage.core.graph import get_graph_operator_class
from sqllineage.core.graph.networkx import NetworkXGraphOperator
from sqllineage.core.graph.rustworkx import RustworkXGraphOperator
from sqllineage.utils.constant import EdgeDirection


@pytest.mark.parametrize(
    "operator_cls", [NetworkXGraphOperator, RustworkXGraphOperator]
)
def test_retrieve_edges_by_vertex_no_label_returns_all_labels(operator_cls):
    # when called without a label filter, every out-edge must be returned,
    # regardless of the individual edge labels
    go = operator_cls()
    go.add_edge_if_not_exist("a", "b", "L1")
    go.add_edge_if_not_exist("a", "c", "L2")
    go.add_edge_if_not_exist("a", "d", "L2")
    edges = go.retrieve_edges_by_vertex("a", EdgeDirection.OUT)
    assert {(edge.target, edge.label) for edge in edges} == {
        ("b", "L1"),
        ("c", "L2"),
        ("d", "L2"),
    }


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
