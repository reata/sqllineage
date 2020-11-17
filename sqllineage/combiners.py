import itertools
from typing import Set, Iterable

import networkx as nx
from networkx import DiGraph

from sqllineage.core import DataSetLineage
from sqllineage.models import Table


class CombinedDataSetResult:
    def __init__(self, graph: DiGraph) -> None:
        """
        The combined lineage result in representation of Directed Acyclic Graph.

        :param graph: the Directed Acyclic Graph holding all the combined lineage result.
        """
        self._graph = graph
        self._selfloop_tables = self.__retrieve_tag_tables("selfloop")
        self._sourceonly_tables = self.__retrieve_tag_tables("source")
        self._targetonly_tables = self.__retrieve_tag_tables("target")

    @property
    def source_tables(self) -> Set[Table]:
        """
        a list of source :class:`sqllineage.models.Table`
        """
        source_tables = {table for table, deg in self._graph.in_degree if deg == 0}.intersection(
            {table for table, deg in self._graph.out_degree if deg > 0}
        )
        source_tables |= self._selfloop_tables
        source_tables |= self._sourceonly_tables
        return source_tables

    @property
    def target_tables(self) -> Set[Table]:
        """
        a list of target :class:`sqllineage.models.Table`
        """
        target_tables = {table for table, deg in self._graph.out_degree if deg == 0}.intersection(
            {table for table, deg in self._graph.in_degree if deg > 0}
        )
        target_tables |= self._selfloop_tables
        target_tables |= self._targetonly_tables
        return target_tables

    @property
    def intermediate_tables(self) -> Set[Table]:
        """
        a list of intermediate :class:`sqllineage.models.Table`
        """
        intermediate_tables = {
            table for table, deg in self._graph.in_degree if deg > 0
        }.intersection({table for table, deg in self._graph.out_degree if deg > 0})
        intermediate_tables -= self.__retrieve_tag_tables("selfloop")
        return intermediate_tables

    @property
    def graph(self) -> DiGraph:
        """
        The DiGraph held by CombinedLineageResult
        """
        return self._graph

    def __retrieve_tag_tables(self, tag) -> Set[Table]:
        return {table for table, attr in self._graph.nodes(data=True) if attr.get("tag") == tag}


def combine_datasets(lineage_results: Iterable[DataSetLineage]) -> CombinedDataSetResult:
    """
    To combine multiple :class:`sqllineage.core.LineageResult` into :class:`sqllineage.combiners.CombinedLineageResult`
    """
    g = DiGraph()
    for lr in lineage_results:
        if lr.drop:
            for table in lr.drop:
                if g.has_node(table) and g.degree[table] == 0:
                    g.remove_node(table)
        elif lr.rename:
            for (table_old, table_new) in lr.rename:
                g = nx.relabel_nodes(g, {table_old: table_new})
        else:
            read, write = lr.read.copy(), lr.write.copy()
            if lr.intermediate:
                read -= lr.intermediate
            if len(read) > 0 and len(write) == 0:
                g.add_nodes_from(read, source=True, tag="source")
            elif len(read) == 0 and len(write) > 0:
                g.add_nodes_from(write, target=True, tag="target")
            else:
                g.add_nodes_from(read)
                g.add_nodes_from(write)
                for source, target in itertools.product(read, write):
                    g.add_edge(source, target)
    for table in {e[0] for e in nx.selfloop_edges(g)}:
        g.nodes[table]["tag"] = "selfloop"
    return CombinedDataSetResult(g)
