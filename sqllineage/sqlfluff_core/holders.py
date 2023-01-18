import itertools
from typing import List, Optional, Set, Tuple, Union

import networkx as nx
from networkx import DiGraph

from sqllineage.sqlfluff_core.models import (
    SqlFluffColumn,
    SqlFluffPath,
    SqlFluffSubQuery,
    SqlFluffTable,
)
from sqllineage.utils.constant import EdgeType, NodeTag

DATASET_CLASSES = (SqlFluffPath, SqlFluffTable)


class SqlFluffColumnLineageMixin:
    """
    Mixin class with 'get_column_lineage' method
    """

    def get_column_lineage(
        self, exclude_subquery=True
    ) -> Set[Tuple[SqlFluffColumn, ...]]:
        """
        Calculate the column lineage of a holder's graph
        :param exclude_subquery: if only 'SqlFluffTable' are considered
        :return: column lineage into a list of tuples
        """
        self.graph: DiGraph  # For mypy attribute checking
        # filter all the column node in the graph
        column_nodes = [n for n in self.graph.nodes if isinstance(n, SqlFluffColumn)]
        column_graph = self.graph.subgraph(column_nodes)
        source_columns = {column for column, deg in column_graph.in_degree if deg == 0}
        # if a column lineage path ends at SubQuery, then it should be pruned
        target_columns = {
            node
            for node, deg in column_graph.out_degree
            if isinstance(node, SqlFluffColumn) and deg == 0
        }
        if exclude_subquery:
            target_columns = {
                node
                for node in target_columns
                if isinstance(node.parent, SqlFluffTable)
            }
        columns = set()
        for (source, target) in itertools.product(source_columns, target_columns):
            simple_paths = list(nx.all_simple_paths(self.graph, source, target))
            for path in simple_paths:
                columns.add(tuple(path))
        return columns


class SqlFluffSubQueryLineageHolder(SqlFluffColumnLineageMixin):
    """
    SubQuery/Query Level Lineage Result.

    SqlFluffSubQueryLineageHolder will hold attributes like read, write, cte

    Each of them is a set of 'SqlFluffTable' or 'SqlFluffSubQuery'.

    This is the most atomic representation of lineage result.
    """

    def __init__(self) -> None:
        self.graph = nx.DiGraph()
        self.extra_subqueries: Set[SqlFluffSubQuery] = set()

    def __or__(self, other):
        self.graph = nx.compose(self.graph, other.graph)
        return self

    def _property_getter(
        self, prop
    ) -> Union[Set[SqlFluffSubQuery], Set[SqlFluffTable]]:
        return {t for t, attr in self.graph.nodes(data=True) if attr.get(prop) is True}

    def _property_setter(self, value, prop) -> None:
        self.graph.add_node(value, **{prop: True})

    @property
    def read(self) -> Set[Union[SqlFluffSubQuery, SqlFluffTable]]:
        return self._property_getter(NodeTag.READ)  # type: ignore

    def add_read(self, value) -> None:
        self._property_setter(value, NodeTag.READ)
        # the same table can be added (in SQL: joined) multiple times with different alias
        if hasattr(value, "alias"):
            self.graph.add_edge(value, value.alias, type=EdgeType.HAS_ALIAS)

    @property
    def write(self) -> Set[Union[SqlFluffSubQuery, SqlFluffTable]]:
        return self._property_getter(NodeTag.WRITE)  # type: ignore

    def add_write(self, value) -> None:
        self._property_setter(value, NodeTag.WRITE)

    @property
    def cte(self) -> Set[SqlFluffSubQuery]:
        return self._property_getter(NodeTag.CTE)  # type: ignore

    def add_cte(self, value) -> None:
        self._property_setter(value, NodeTag.CTE)

    def add_column_lineage(self, src: SqlFluffColumn, tgt: SqlFluffColumn) -> None:
        """
        Add column lineage between to given 'SqlFluffColumn'
        :param src: source 'SqlFluffColumn'
        :param tgt: target 'SqlFluffColumn'
        """
        self.graph.add_edge(src, tgt, type=EdgeType.LINEAGE)
        self.graph.add_edge(tgt.parent, tgt, type=EdgeType.HAS_COLUMN)
        if src.parent is not None:
            self.graph.add_edge(src.parent, src, type=EdgeType.HAS_COLUMN)


class SqlFluffStatementLineageHolder(
    SqlFluffSubQueryLineageHolder, SqlFluffColumnLineageMixin
):
    """
    Statement Level Lineage Result.

    Based on 'SqlFluffSubQueryLineageHolder' and 'StatementLineageHolder' holds extra attributes like drop and rename

    For drop, it is a set of 'SqlFluffTable'.

    For rename, it is a set of tuples of 'SqlFluffTable', with the first table being original table before renaming and
    the latter after renaming.
    """

    def __str__(self):
        return "\n".join(
            f"table {attr}: {sorted(getattr(self, attr), key=lambda x: str(x)) if getattr(self, attr) else '[]'}"
            for attr in ["read", "write", "cte", "drop", "rename"]
        )

    def __repr__(self):
        return str(self)

    @property
    def read(self) -> Set[SqlFluffTable]:  # type: ignore
        return {t for t in super().read if isinstance(t, DATASET_CLASSES)}

    @property
    def write(self) -> Set[SqlFluffTable]:  # type: ignore
        return {t for t in super().write if isinstance(t, DATASET_CLASSES)}

    @property
    def drop(self) -> Set[SqlFluffTable]:
        return self._property_getter(NodeTag.DROP)  # type: ignore

    def add_drop(self, value) -> None:
        self._property_setter(value, NodeTag.DROP)

    @property
    def rename(self) -> Set[Tuple[SqlFluffTable, SqlFluffTable]]:
        return {
            (src, tgt)
            for src, tgt, attr in self.graph.edges(data=True)
            if attr.get("type") == EdgeType.RENAME
        }

    def add_rename(self, src: SqlFluffTable, tgt: SqlFluffTable) -> None:
        """
        Add rename of a source 'SqlFluffColumn' into a target 'SqlFluffColumn'
        :param src: source 'SqlFluffTable'
        :param tgt: target 'SqlFluffTable'
        """
        self.graph.add_edge(src, tgt, type=EdgeType.RENAME)

    @staticmethod
    def of(holder: SqlFluffSubQueryLineageHolder) -> "SqlFluffStatementLineageHolder":
        """
        Build a 'SqlFluffStatementLineageHolder' object
        :param holder: 'SqlFluffSubQueryLineageHolder' to hold lineage
        :return: 'SqlFluffStatementLineageHolder' object
        """
        stmt_holder = SqlFluffStatementLineageHolder()
        stmt_holder.graph = holder.graph
        return stmt_holder


class SqlFluffLineageHolder(SqlFluffColumnLineageMixin):
    """
    Lineage Result
    """

    def __init__(self, graph: DiGraph):
        """
        The combined lineage result in representation of Directed Acyclic Graph.
        :param graph: the Directed Acyclic Graph holding all the combined lineage result.
        """
        self.graph = graph
        self._selfloop_tables = self.__retrieve_tag_tables(NodeTag.SELFLOOP)
        self._sourceonly_tables = self.__retrieve_tag_tables(NodeTag.SOURCE_ONLY)
        self._targetonly_tables = self.__retrieve_tag_tables(NodeTag.TARGET_ONLY)

    @property
    def table_lineage_graph(self) -> DiGraph:
        """
        :return the table level DiGraph held by 'SqlFluffLineageHolder'
        """
        table_nodes = [n for n in self.graph.nodes if isinstance(n, DATASET_CLASSES)]
        return self.graph.subgraph(table_nodes)

    @property
    def column_lineage_graph(self) -> DiGraph:
        """
        :return the column level DiGraph held by 'SqlFluffLineageHolder'
        """
        column_nodes = [n for n in self.graph.nodes if isinstance(n, SqlFluffColumn)]
        return self.graph.subgraph(column_nodes)

    @property
    def source_tables(self) -> Set[SqlFluffTable]:
        """
        :return a list of source 'SqlFluffTable'
        """
        source_tables = {
            table for table, deg in self.table_lineage_graph.in_degree if deg == 0
        }.intersection(
            {table for table, deg in self.table_lineage_graph.out_degree if deg > 0}
        )
        source_tables |= self._selfloop_tables
        source_tables |= self._sourceonly_tables
        return source_tables

    @property
    def target_tables(self) -> Set[SqlFluffTable]:
        """
        :return a list of target 'SqlFluffTable'
        """
        target_tables = {
            table for table, deg in self.table_lineage_graph.out_degree if deg == 0
        }.intersection(
            {table for table, deg in self.table_lineage_graph.in_degree if deg > 0}
        )
        target_tables |= self._selfloop_tables
        target_tables |= self._targetonly_tables
        return target_tables

    @property
    def intermediate_tables(self) -> Set[SqlFluffTable]:
        """
        :return a list of intermediate 'SqlFluffTable'
        """
        intermediate_tables = {
            table for table, deg in self.table_lineage_graph.in_degree if deg > 0
        }.intersection(
            {table for table, deg in self.table_lineage_graph.out_degree if deg > 0}
        )
        intermediate_tables -= self.__retrieve_tag_tables(NodeTag.SELFLOOP)
        return intermediate_tables

    def __retrieve_tag_tables(self, tag) -> Set[Union[SqlFluffPath, SqlFluffTable]]:
        return {
            table
            for table, attr in self.graph.nodes(data=True)
            if attr.get(tag) is True and isinstance(table, DATASET_CLASSES)
        }

    @staticmethod
    def _get_column_if_related_to_parent(
        g: DiGraph,
        raw_name: str,
        parent: Union[SqlFluffTable, SqlFluffSubQuery],
    ) -> Optional[SqlFluffColumn]:
        src_col = SqlFluffColumn(raw_name)
        src_col.parent = parent
        return src_col if g.has_edge(parent, src_col) else None

    @classmethod
    def _build_digraph(cls, holders: List[SqlFluffStatementLineageHolder]) -> DiGraph:
        """
        To assemble multiple 'SqlFluffStatementLineageHolder' into 'SqlFluffLineageHolder'
        :param holders: a list of 'SqlFluffStatementLineageHolder'
        :return: the DiGraph held
        """
        g = DiGraph()
        for holder in holders:
            g = nx.compose(g, holder.graph)
            if holder.drop:
                for table in holder.drop:
                    if g.has_node(table) and g.degree[table] == 0:
                        g.remove_node(table)
            elif holder.rename:
                for (table_old, table_new) in holder.rename:
                    g = nx.relabel_nodes(g, {table_old: table_new})
                    g.remove_edge(table_new, table_new)
                    if g.degree[table_new] == 0:
                        g.remove_node(table_new)
            else:
                read, write = holder.read, holder.write
                if len(read) > 0 and len(write) == 0:
                    # source only table comes from SELECT statement
                    nx.set_node_attributes(
                        g, {table: True for table in read}, NodeTag.SOURCE_ONLY
                    )
                elif len(read) == 0 and len(write) > 0:
                    # target only table comes from case like: 1) INSERT/UPDATE constant values; 2) CREATE TABLE
                    nx.set_node_attributes(
                        g, {table: True for table in write}, NodeTag.TARGET_ONLY
                    )
                else:
                    for source, target in itertools.product(read, write):
                        g.add_edge(source, target, type=EdgeType.LINEAGE)
        nx.set_node_attributes(
            g,
            {table: True for table in {e[0] for e in nx.selfloop_edges(g)}},
            NodeTag.SELFLOOP,
        )
        # find all the columns that we can't assign accurately to a parent table (with multiple parent candidates)
        unresolved_cols = [
            (s, t)
            for s, t in g.edges
            if isinstance(s, SqlFluffColumn) and len(s.parent_candidates) > 1
        ]
        for unresolved_col, tgt_col in unresolved_cols:
            # check if there's only one parent candidate contains the column with same name
            src_cols = []
            for parent in unresolved_col.parent_candidates:
                src_col = cls._get_column_if_related_to_parent(
                    g, unresolved_col.raw_name, parent
                )
                if src_col:
                    src_cols.append(src_col)
            if len(src_cols) == 1:
                g.add_edge(src_cols[0], tgt_col, type=EdgeType.LINEAGE)
                g.remove_edge(unresolved_col, tgt_col)
        # when unresolved column got resolved, it will be orphan node, and we can remove it
        for node in [n for n, deg in g.degree if deg == 0]:
            if isinstance(node, SqlFluffColumn) and len(node.parent_candidates) > 1:
                g.remove_node(node)
        return g

    @staticmethod
    def of(holders: List[SqlFluffStatementLineageHolder]):
        """
        To assemble multiple 'SqlFluffStatementLineageHolder' into 'SqlFluffLineageHolder'
        :param holders: a list of 'SqlFluffStatementLineageHolder'
        :return: a 'SqlFluffLineageHolder' object
        """
        g = SqlFluffLineageHolder._build_digraph(holders)
        return SqlFluffLineageHolder(g)
