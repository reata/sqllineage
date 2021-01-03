from typing import Any, Dict, List

from networkx import DiGraph


def to_cytoscape(graph: DiGraph) -> Dict[str, List[Any]]:
    return {
        "nodes": [{"data": {"id": str(node)}} for node in graph.nodes],
        "edges": [
            {"data": {"source": str(edge[0]), "target": str(edge[1])}}
            for edge in graph.edges
        ],
    }
