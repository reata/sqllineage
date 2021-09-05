import itertools
from typing import Set, TYPE_CHECKING, Tuple, Union

import networkx as nx

from sqllineage.models import Column, SubQuery, Table


class LineageResult:
    """
    Statement Level Lineage Result.

    LineageResult will hold attributes like read, write, rename, drop, intermediate.

    Each of them is a Set[:class:`sqllineage.models.Table`] except for rename.

    For rename, it a Set[Tuple[:class:`sqllineage.models.Table`, :class:`sqllineage.models.Table`]], with the first
    table being original table before renaming and the latter after renaming.

    This is the most atomic representation of lineage result.
    """

    __slots__ = ["read", "write", "rename", "drop", "intermediate", "graph"]
    if TYPE_CHECKING:
        read = write = drop = intermediate = set()  # type: Set[Union[Table, SubQuery]]
        rename = set()  # type: Set[Tuple[Table, Table]]

    def __init__(self) -> None:
        for attr in self.__slots__:
            if attr == "graph":
                self.graph = nx.DiGraph()
            else:
                setattr(self, attr, set())

    def __or__(self, other):
        lineage_result = LineageResult()
        for attr in self.__slots__:
            if attr == "rename":
                lineage_result.rename = self.rename.union(other.rename)
            elif attr == "graph":
                lineage_result.graph = nx.compose(self.graph, other.graph)
            else:
                setattr(
                    lineage_result,
                    attr,
                    {
                        t
                        for t in getattr(self, attr).union(getattr(other, attr))
                        if isinstance(t, Table)
                    },
                )
        return lineage_result

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
