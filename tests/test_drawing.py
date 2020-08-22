from unittest.mock import patch

from networkx import DiGraph

from sqllineage.drawing import draw_lineage_graph


@patch("matplotlib.pyplot.show")
def test_dummy(_):
    g = DiGraph()
    g.add_node(1)
    g.add_edge(1, 1)
    draw_lineage_graph(g)
