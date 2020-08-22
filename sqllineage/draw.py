import matplotlib.pyplot as plt
import networkx as nx
from matplotlib.colors import colorConverter
from matplotlib.patches import FancyArrowPatch
from matplotlib.path import Path
from networkx import DiGraph


def draw_lineage_graph(graph: DiGraph) -> None:
    pos = nx.nx_agraph.graphviz_layout(graph, prog="dot", args="-Grankdir='LR'")
    edge_color = "#9ab5c7"
    arrowsize = 10
    nx.draw(
        graph,
        pos=pos,
        with_labels=True,
        edge_color=edge_color,
        arrowsize=arrowsize,
        node_color="#3499d9",
        node_size=30,
        font_color="#35393e",
        font_size=10,
    )
    # selfloop edges
    radius = 10
    for edge in nx.selfloop_edges(graph):
        x, y = pos[edge[0]]
        arrow = FancyArrowPatch(
            path=Path.circle((x, y + radius), radius),
            arrowstyle="-|>",
            color=colorConverter.to_rgba_array(edge_color)[0],
            mutation_scale=arrowsize,
            linewidth=1.0,
            zorder=1,
        )
        plt.gca().add_patch(arrow)
    plt.show()
