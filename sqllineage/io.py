from typing import Dict, List

from networkx import DiGraph


def to_cytoscape(graph: DiGraph, compound=False) -> List[Dict[str, Dict[str, str]]]:
    """
    compound nodes is used to group nodes together to their parent.
    See https://js.cytoscape.org/#notation/compound-nodes for reference.
    """
    if compound:
        parents_dict = {
            node.parent: {
                "name": str(node.parent) if node.parent is not None else "<unknown>",
                "type": type(node.parent).__name__
                if node.parent is not None
                else "Table or SubQuery",
            }
            for node in graph.nodes
        }
        nodes = [
            {
                "data": {
                    "id": str(node),
                    "parent": parents_dict[node.parent]["name"],
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
    edges = [
        {"data": {"id": f"e{i}", "source": str(edge[0]), "target": str(edge[1])}}
        for i, edge in enumerate(graph.edges)
    ]
    return nodes + edges
