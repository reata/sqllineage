import logging

import networkx as nx
from networkx import DiGraph

logger = logging.getLogger(__name__)


def draw_lineage_graph(graph: DiGraph) -> None:
    try:
        import matplotlib.pyplot as plt
        from matplotlib.colors import colorConverter
        from matplotlib.patches import FancyArrowPatch
        from matplotlib.path import Path
    except ImportError as e:
        raise ImportError("Matplotlib required for draw()") from e
    except RuntimeError:
        logger.error("Matplotlib unable to open display")
        raise
    try:
        import pygraphviz  # noqa
    except ImportError as e:
        raise ImportError("requires pygraphviz") from e
    pos = nx.nx_agraph.graphviz_layout(graph, prog="dot", args="-Grankdir='LR'")
    edge_color, node_color, font_color = "#9ab5c7", "#3499d9", "#35393e"
    arrowsize = font_size = radius = 10
    node_size = 30
    ha, va = "left", "bottom"
    nx.draw(
        graph,
        pos=pos,
        with_labels=True,
        edge_color=edge_color,
        arrowsize=arrowsize,
        node_color=node_color,
        node_size=node_size,
        font_color=font_color,
        font_size=font_size,
        horizontalalignment=ha,
        verticalalignment=va,
    )
    # selfloop edges
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
