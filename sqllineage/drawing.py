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


def draw_lineage_graph2(graph: DiGraph) -> None:
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

    _color_source = "#75ff33"
    _color_target = "#ff5733"
    _color_both = "#bd33ff"
    _color_neither = "#33dbff"

    sources = [k for k, v in nx.get_node_attributes(graph, "source").items() if v]
    targets = [k for k, v in nx.get_node_attributes(graph, "target").items() if v]
    color_map = []
    for node in graph:
        if node in sources:
            if node in targets:
                color_map.append(_color_both)
            else:
                color_map.append(_color_source)
        else:
            if node in targets:
                color_map.append(_color_target)
            else:
                color_map.append(_color_neither)

    pos = nx.nx_agraph.graphviz_layout(graph, prog="dot", args="-Grankdir=LR")
    edge_color, font_color = "#9ab5c7", "#35393e"  # "#3499d9"
    arrowsize = font_size = radius = 10
    node_size = 30
    ha, va = "left", "bottom"

    nx.draw(
        graph,
        pos=pos,
        with_labels=True,
        edge_color=edge_color,
        arrowsize=arrowsize,
        node_color=color_map,
        node_size=node_size,
        font_color=font_color,
        font_size=font_size,
        horizontalalignment=ha,
        verticalalignment=va,
    )

    # # selfloop edges
    # for edge in nx.selfloop_edges(graph):
    #     x, y = pos[edge[0]]
    #     arrow = FancyArrowPatch(
    #         path=Path.circle((x, y + radius), radius),
    #         arrowstyle="-|>",
    #         color=colorConverter.to_rgba_array(edge_color)[0],
    #         mutation_scale=arrowsize,
    #         linewidth=1.0,
    #         zorder=1,
    #     )
    #     plt.gca().add_patch(arrow)

    axis = plt.gca()
    axis.set_xlim([1.2 * x for x in axis.get_xlim()])
    axis.set_ylim([1.1 * y for y in axis.get_ylim()])

    plt.show()
