# import matplotlib.pyplot as plt
# import networkx as nx
# from matplotlib.colors import colorConverter
# from matplotlib.patches import FancyArrowPatch
# from matplotlib.path import Path
#
# from sqllineage.models import Schema, Table
#
# Tables = {
#     i + 1: Table("table{}".format(i + 1), schema=Schema("default")) for i in range(16)
# }
#
# G = nx.DiGraph()
# for table in Tables.values():
#     G.add_node(table)
#
#
# def add_edge(ith, jth):
#     G.add_edge(Tables[ith], Tables[jth])
#
#
# for (i, j) in [
#     (1, 8),
#     (2, 8),
#     (3, 9),
#     (4, 9),
#     (5, 13),
#     (6, 13),
#     (7, 13),
#     (9, 14),
#     (10, 14),
#     (11, 14),
#     (12, 14),
#     (14, 15),
#     (8, 16),
#     (15, 16),
#     (13, 16),
#     (16, 16),
# ]:
#     add_edge(i, j)
#
# # 绘图
# pos = nx.nx_agraph.graphviz_layout(G, prog="dot", args="-Grankdir='LR'")
# edge_color = "#9ab5c7"
# arrowsize = 10
# nx.draw(
#     G,
#     pos=pos,
#     with_labels=True,
#     edge_color=edge_color,
#     arrowsize=arrowsize,
#     node_color="#3499d9",
#     node_size=30,
#     font_color="#35393e",
#     font_size=10,
# )
# # selfloop edges
# radius = 10
# for edge in nx.selfloop_edges(G):
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
# plt.show()
#
# # 多图合并
# G1 = nx.DiGraph()
# G1.add_nodes_from([Tables[3], Tables[9]])
# G1.add_edge(Tables[3], Tables[9])
# G2 = nx.DiGraph()
# G2.add_nodes_from([Tables[4], Tables[9]])
# G2.add_edge(Tables[4], Tables[9])
# G3 = nx.compose(G1, G2)
#
# # source, target, intermediate 节点的识别
# source_tables = {table for table, deg in G.in_degree if deg == 0}.intersection(
#     {table for table, deg in G.out_degree if deg > 0}
# )
# target_tables = {table for table, deg in G.out_degree if deg == 0}.intersection(
#     {table for table, deg in G.in_degree if deg > 0}
# )
# intermediate_tables = {table for table, deg in G.in_degree if deg > 0}.intersection(
#     {table for table, deg in G.out_degree if deg > 0}
# )
# selfloop_tables = {e[0] for e in nx.selfloop_edges(G)}
# source_tables |= selfloop_tables
# target_tables |= selfloop_tables
# intermediate_tables -= selfloop_tables
