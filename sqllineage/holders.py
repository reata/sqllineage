import itertools
from typing import Set, TYPE_CHECKING, Tuple, Union

import networkx as nx
from networkx import DiGraph

from sqllineage.models import Column, SubQuery, Table


class SQLLineageHolder:
    def __init__(self, graph: DiGraph):
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
        source_tables = {
            table for table, deg in self._graph.in_degree if deg == 0
        }.intersection({table for table, deg in self._graph.out_degree if deg > 0})
        source_tables |= self._selfloop_tables
        source_tables |= self._sourceonly_tables
        return source_tables

    @property
    def target_tables(self) -> Set[Table]:
        """
        a list of target :class:`sqllineage.models.Table`
        """
        target_tables = {
            table for table, deg in self._graph.out_degree if deg == 0
        }.intersection({table for table, deg in self._graph.in_degree if deg > 0})
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
    def lineage_graph(self) -> DiGraph:
        """
        The DiGraph held by SQLLineageHolder
        """
        return self._graph

    def __retrieve_tag_tables(self, tag) -> Set[Table]:
        return {
            table
            for table, attr in self._graph.nodes(data=True)
            if attr.get("tag") == tag
        }


class StatementLineageHolder:
    """
    Statement Level Lineage Result.

    StatementLineageHolder will hold attributes like read, write, rename, drop, intermediate.

    Each of them is a Set[:class:`sqllineage.models.Table`] except for rename.

    For rename, it a Set[Tuple[:class:`sqllineage.models.Table`, :class:`sqllineage.models.Table`]], with the first
    table being original table before renaming and the latter after renaming.

    This is the most atomic representation of lineage result.
    """

    __slots__ = ["read", "write", "rename", "drop", "intermediate", "graph"]
    if TYPE_CHECKING:
        read = write = drop = intermediate = set()  # type: Set[Table]
        rename = set()  # type: Set[Tuple[Table, Table]]

    def __init__(self) -> None:
        for attr in self.__slots__:
            if attr == "graph":
                self.graph = nx.DiGraph()
            else:
                setattr(self, attr, set())

    def __str__(self):
        return "\n".join(
            f"table {attr}: {sorted(getattr(self, attr), key=lambda x: str(x)) if getattr(self, attr) else '[]'}"
            for attr in self.__slots__
        )

    def __repr__(self):
        return str(self)

    @property
    def column(self) -> Set[Tuple[Column, Column]]:
        columns = set()
        source_columns = {column for column, deg in self.graph.in_degree if deg == 0}
        # if a column lineage path ends at SubQuery, then it should be pruned
        target_columns = {
            column
            for column, deg in self.graph.out_degree
            if deg == 0 and isinstance(column.parent, Table)
        }
        for (source, target) in itertools.product(source_columns, target_columns):
            if list(nx.all_simple_paths(self.graph, source, target)):
                columns.add((source, target))
        return columns


class SubQueryLineageHolder:
    __slots__ = ["read", "write", "intermediate", "graph"]
    if TYPE_CHECKING:
        read = write = intermediate = set()  # type: Set[Union[Table, SubQuery]]

    def __init__(self) -> None:
        for attr in self.__slots__:
            if attr == "graph":
                self.graph = nx.DiGraph()
            else:
                setattr(self, attr, set())

    def to_stmt_lineage_holder(self) -> StatementLineageHolder:
        holder = StatementLineageHolder()
        for attr in self.__slots__:
            setattr(holder, attr, getattr(self, attr))
        return holder
