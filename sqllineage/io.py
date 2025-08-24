from typing import Any

from networkx import DiGraph

from sqllineage.utils.constant import LineageLevel


def to_cytoscape(graph: DiGraph, compound=False) -> list[dict[str, dict[str, Any]]]:
    """
    compound nodes is used to group nodes together to their parent.
    See https://js.cytoscape.org/#notation/compound-nodes for reference.
    """
    if compound:
        parents_dict = {
            node.parent: {
                "name": str(node.parent) if node.parent is not None else "<unknown>",
                "type": (
                    type(node.parent).__name__
                    if node.parent is not None
                    else "Table or SubQuery"
                ),
            }
            for node in graph.nodes
        }
        nodes = [
            {
                "data": {
                    "id": str(node),
                    "parent": parents_dict[node.parent]["name"],
                    "parent_candidates": [
                        {"name": str(p), "type": type(p).__name__}
                        for p in node.parent_candidates
                    ],
                    "type": type(node).__name__,
                }
            }
            for node in graph.nodes
        ]
        nodes += [
            {"data": {"id": attr["name"], "type": attr["type"]}}
            for _, attr in parents_dict.items()
        ]
    else:
        nodes = [{"data": {"id": str(node)}} for node in graph.nodes]
    edges: list[dict[str, dict[str, Any]]] = [
        {"data": {"id": f"e{i}", "source": str(edge[0]), "target": str(edge[1])}}
        for i, edge in enumerate(graph.edges)
    ]
    return nodes + edges


def to_reactflow(graph: DiGraph, level: str) -> dict[str, list[dict[str, Any]]]:
    """
    graph visualization using reactflow
    """
    if level == LineageLevel.COLUMN:
        parent_nodes = [
            {
                "id": str(parent),
                "data": {"label": str(parent)},
                "type": "group",
            }
            for parent in {node.parent for node in graph.nodes}
        ]
        column_nodes = [
            {
                "id": str(node),
                "data": {"label": str(node)},
                "type": "default",
                "parentId": str(node.parent),
                "extent": "parent",
            }
            for node in graph.nodes
        ]
        # parent nodes should come first for reactflow to render correctly
        nodes = parent_nodes + column_nodes
    else:
        nodes = [
            {"id": str(node), "data": {"label": str(node)}, "type": "default"}
            for node in graph.nodes
        ]
    edges: list[dict[str, Any]] = [
        {"id": f"{u}->{v}", "source": str(u), "target": str(v)} for u, v in graph.edges
    ]
    return {"nodes": nodes, "edges": edges}
