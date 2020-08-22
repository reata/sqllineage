from networkx import DiGraph

from sqllineage.drawing import draw_lineage_graph


def test_dummy():
    g = DiGraph()
    g.add_node(1)
    g.add_edge(1, 1)
    draw_lineage_graph(g)
