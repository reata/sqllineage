import itertools

import networkx as nx
from networkx import DiGraph

from sqllineage.holders import (
    SQLLineageHolder,
    StatementLineageHolder,
    SubQueryLineageHolder,
)
from sqllineage.models import Table


def combine_subquery_lineage(*args: SubQueryLineageHolder) -> SubQueryLineageHolder:
    holder = SubQueryLineageHolder()
    for sq_holder in args:
        for attr in SubQueryLineageHolder.__slots__:
            if attr == "graph":
                holder.graph = nx.compose(holder.graph, sq_holder.graph)
            else:
                setattr(
                    holder,
                    attr,
                    {
                        t
                        for t in getattr(holder, attr).union(getattr(sq_holder, attr))
                        if isinstance(t, Table)
                    },
                )
    return holder


def combine_statement_lineage(*args: StatementLineageHolder) -> SQLLineageHolder:
    """
    To combine multiple :class:`sqllineage.holders.StatementLineageHolder` into
    :class:`sqllineage.holders.SQLLineageHolder`
    """
    g = DiGraph()
    for holder in args:
        if holder.drop:
            for table in holder.drop:
                if g.has_node(table) and g.degree[table] == 0:
                    g.remove_node(table)
        elif holder.rename:
            for (table_old, table_new) in holder.rename:
                g = nx.relabel_nodes(g, {table_old: table_new})
        else:
            read, write = holder.read.copy(), holder.write.copy()
            if holder.intermediate:
                read -= holder.intermediate
            if len(read) > 0 and len(write) == 0:
                g.add_nodes_from(read, tag="source")
            elif len(read) == 0 and len(write) > 0:
                g.add_nodes_from(write, tag="target")
            else:
                g.add_nodes_from(read)
                g.add_nodes_from(write)
                for source, target in itertools.product(read, write):
                    g.add_edge(source, target)
    for table in {e[0] for e in nx.selfloop_edges(g)}:
        g.nodes[table]["tag"] = "selfloop"
    return SQLLineageHolder(g)
