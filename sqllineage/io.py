from typing import Dict, List

from networkx import DiGraph


def to_cytoscape(graph: DiGraph) -> List[Dict[str, Dict[str, str]]]:
    nodes = [{"data": {"id": str(node)}} for node in graph.nodes]
    edges = [
        {"data": {"id": f"e{i}", "source": str(edge[0]), "target": str(edge[1])}}
        for i, edge in enumerate(graph.edges)
    ]
    return nodes + edges
