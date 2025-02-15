import itertools
from typing import Optional, Union

import networkx as nx
from networkx import DiGraph

from sqllineage.core.metadata_provider import MetaDataProvider
from sqllineage.core.models import Column, Path, Schema, SubQuery, Table
from sqllineage.utils.constant import EdgeTag, EdgeType, NodeTag

DATASET_CLASSES = (Path, Table)


class ColumnLineageMixin:
    def get_column_lineage(
        self, exclude_path_ending_in_subquery=True, exclude_subquery_columns=False
    ) -> set[tuple[Column, ...]]:
        """
        :param exclude_path_ending_in_subquery:  exclude_subquery rename to exclude_path_ending_in_subquery
               exclude column from SubQuery in the ending path
        :param exclude_subquery_columns: exclude column from SubQuery in the path.

        return a list of column tuple :class:`sqllineage.models.Column`
        """
        self.graph: DiGraph  # For mypy attribute checking
        # filter all the column node in the graph
        column_nodes = [n for n in self.graph.nodes if isinstance(n, Column)]
        column_graph = self.graph.subgraph(column_nodes)
        source_columns = {column for column, deg in column_graph.in_degree if deg == 0}
        # if a column lineage path ends at SubQuery, then it should be pruned
        target_columns = {
            node
            for node, deg in column_graph.out_degree
            if isinstance(node, Column) and deg == 0
        }
        if exclude_path_ending_in_subquery:
            target_columns = {
                node for node in target_columns if isinstance(node.parent, Table)
            }
        columns = set()
        for source, target in itertools.product(source_columns, target_columns):
            simple_paths = list(nx.all_simple_paths(self.graph, source, target))
            for path in simple_paths:
                if exclude_subquery_columns:
                    path = [
                        node for node in path if not isinstance(node.parent, SubQuery)
                    ]
                    if len(path) > 1:
                        columns.add(tuple(path))
                else:
                    columns.add(tuple(path))
        return columns


class SubQueryLineageHolder(ColumnLineageMixin):
    """
    SubQuery/Query Level Lineage Result.

    SubQueryLineageHolder will hold attributes like read, write, cte.

    Each of them is a set[:class:`sqllineage.core.models.Table`].

    This is the most atomic representation of lineage result.
    """

    def __init__(self) -> None:
        self.graph = nx.DiGraph()

    def __or__(self, other):
        self.graph = nx.compose(self.graph, other.graph)
        return self

    def _property_getter(self, prop) -> set[Union[SubQuery, Table]]:
        return {t for t, attr in self.graph.nodes(data=True) if attr.get(prop) is True}

    def _property_setter(self, value, prop) -> None:
        self.graph.add_node(value, **{prop: True})

    @property
    def read(self) -> set[Union[SubQuery, Table]]:
        return self._property_getter(NodeTag.READ)

    def add_read(self, value) -> None:
        self._property_setter(value, NodeTag.READ)
        # the same table can be added (in SQL: joined) multiple times with different alias
        if hasattr(value, "alias"):
            self.graph.add_edge(value, value.alias, type=EdgeType.HAS_ALIAS)

    @property
    def write(self) -> set[Union[SubQuery, Table]]:
        # SubQueryLineageHolder.write can return a single SubQuery or Table, or both when __or__ together.
        # This is different from StatementLineageHolder.write, where Table is the only possibility.
        return self._property_getter(NodeTag.WRITE)

    def add_write(self, value) -> None:
        self._property_setter(value, NodeTag.WRITE)

    @property
    def cte(self) -> set[SubQuery]:
        return self._property_getter(NodeTag.CTE)  # type: ignore

    def add_cte(self, value) -> None:
        self._property_setter(value, NodeTag.CTE)

    @property
    def write_columns(self) -> list[Column]:
        """
        return a list of columns that write table contains.
        It's either manually added via `add_write_column` if specified in DML
        or automatic added via `add_column_lineage` after parsing from SELECT
        """
        tgt_cols = []
        if tgt_tbl := self._get_target_table():
            tgt_col_with_idx: list[tuple[Column, int]] = sorted(
                [
                    (col, attr.get(EdgeTag.INDEX, 0))
                    for tbl, col, attr in self.graph.out_edges(tgt_tbl, data=True)
                    if attr["type"] == EdgeType.HAS_COLUMN
                ],
                key=lambda x: x[1],
            )
            tgt_cols = [x[0] for x in tgt_col_with_idx]
        return tgt_cols

    def add_write_column(self, *tgt_cols: Column) -> None:
        """
        in case of DML with column specified, like:

        .. code-block:: sql

            INSERT INTO tab1 (col1, col2)
            SELECT col3, col4

        this method is called to make sure tab1 has column col1 and col2 instead of col3 and col4
        """
        if self.write:
            tgt_tbl = list(self.write)[0]
            for idx, tgt_col in enumerate(tgt_cols):
                tgt_col.parent = tgt_tbl
                self.graph.add_edge(
                    tgt_tbl, tgt_col, type=EdgeType.HAS_COLUMN, **{EdgeTag.INDEX: idx}
                )

    def add_column_lineage(self, src: Column, tgt: Column) -> None:
        """
        link source column to target.
        """
        self.graph.add_edge(src, tgt, type=EdgeType.LINEAGE)
        self.graph.add_edge(tgt.parent, tgt, type=EdgeType.HAS_COLUMN)
        if src.parent is not None:
            # starting NetworkX v2.6, None is not allowed as node, see https://github.com/networkx/networkx/pull/4892
            self.graph.add_edge(src.parent, src, type=EdgeType.HAS_COLUMN)

    def get_table_columns(self, table: Union[Table, SubQuery]) -> list[Column]:
        return [
            tgt
            for (src, tgt, edge_type) in self.graph.out_edges(nbunch=table, data="type")
            if edge_type == EdgeType.HAS_COLUMN
            and isinstance(tgt, Column)
            and tgt.raw_name != "*"
        ]

    def expand_wildcard(self, metadata_provider: MetaDataProvider) -> None:
        if tgt_table := self._get_target_table():
            for column in self.write_columns:
                if column.raw_name == "*":
                    tgt_wildcard = column
                    for src_wildcard in self.get_source_columns(tgt_wildcard):
                        if source_table := src_wildcard.parent:
                            src_table_columns = []
                            if isinstance(source_table, SubQuery):
                                # the columns of SubQuery can be inferred from graph
                                src_table_columns = self.get_table_columns(source_table)
                            elif isinstance(source_table, Table) and metadata_provider:
                                # search by metadata service
                                src_table_columns = metadata_provider.get_table_columns(
                                    source_table
                                )
                            if src_table_columns:
                                self._replace_wildcard(
                                    tgt_table,
                                    src_table_columns,
                                    tgt_wildcard,
                                    src_wildcard,
                                )

    def get_alias_mapping_from_table_group(
        self, table_group: list[Union[Path, Table, SubQuery]]
    ) -> dict[str, Union[Path, Table, SubQuery]]:
        """
        A table can be referred to as alias, table name, or database_name.table_name, create the mapping here.
        For SubQuery, it's only alias then.
        """
        alias_map = {
            tgt: src
            for src, tgt, attr in self.graph.edges(data=True)
            if attr.get("type") == EdgeType.HAS_ALIAS and src in table_group
        }
        unqualified_map = {
            table.raw_name: table for table in table_group if isinstance(table, Table)
        }
        qualified_map = {
            str(table): table for table in table_group if isinstance(table, Table)
        }
        return alias_map | unqualified_map | qualified_map

    def _get_target_table(self) -> Optional[Union[SubQuery, Table]]:
        table = None
        if write_only := self.write.difference(self.read):
            table = next(iter(write_only))
        return table

    def get_source_columns(self, node: Column) -> list[Column]:
        return [
            src
            for (src, tgt, edge_type) in self.graph.in_edges(nbunch=node, data="type")
            if edge_type == EdgeType.LINEAGE and isinstance(src, Column)
        ]

    def _replace_wildcard(
        self,
        tgt_table: Union[Table, SubQuery],
        src_table_columns: list[Column],
        tgt_wildcard: Column,
        src_wildcard: Column,
    ) -> None:
        target_columns = self.get_table_columns(tgt_table)
        for src_col in src_table_columns:
            new_column = Column(src_col.raw_name)
            new_column.parent = tgt_table
            if new_column in target_columns or src_col.raw_name == "*":
                continue
            self.graph.add_edge(tgt_table, new_column, type=EdgeType.HAS_COLUMN)
            self.graph.add_edge(src_col.parent, src_col, type=EdgeType.HAS_COLUMN)
            self.graph.add_edge(src_col, new_column, type=EdgeType.LINEAGE)
        # remove wildcard
        if self.graph.has_node(tgt_wildcard):
            self.graph.remove_node(tgt_wildcard)
        if self.graph.has_node(src_wildcard):
            self.graph.remove_node(src_wildcard)


class StatementLineageHolder(SubQueryLineageHolder, ColumnLineageMixin):
    """
    Statement Level Lineage Result.

    Based on SubQueryLineageHolder, StatementLineageHolder holds extra attributes like drop and rename

    For drop, it is a set[:class:`sqllineage.core.models.Table`].

    For rename, it a set[tuple[:class:`sqllineage.core.models.Table`, :class:`sqllineage.core.models.Table`]],
    with the first table being original table before renaming and the latter after renaming.
    """

    def __str__(self):
        return "\n".join(
            f"table {attr}: {sorted(getattr(self, attr), key=lambda x: str(x)) if getattr(self, attr) else '[]'}"
            for attr in ["read", "write", "cte", "drop", "rename"]
        )

    def __repr__(self):
        return str(self)

    @property
    def read(self) -> set[Table]:  # type: ignore
        return {t for t in super().read if isinstance(t, DATASET_CLASSES)}

    @property
    def write(self) -> set[Table]:  # type: ignore
        return {t for t in super().write if isinstance(t, DATASET_CLASSES)}

    @property
    def drop(self) -> set[Table]:
        return self._property_getter(NodeTag.DROP)  # type: ignore

    def add_drop(self, value) -> None:
        self._property_setter(value, NodeTag.DROP)

    @property
    def rename(self) -> set[tuple[Table, Table]]:
        return {
            (src, tgt)
            for src, tgt, attr in self.graph.edges(data=True)
            if attr.get("type") == EdgeType.RENAME
        }

    def add_rename(self, src: Table, tgt: Table) -> None:
        self.graph.add_edge(src, tgt, type=EdgeType.RENAME)

    @staticmethod
    def of(holder: SubQueryLineageHolder) -> "StatementLineageHolder":
        stmt_holder = StatementLineageHolder()
        stmt_holder.graph = holder.graph
        return stmt_holder


class SQLLineageHolder(ColumnLineageMixin):
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
        The table level DiGraph held by SQLLineageHolder
        """
        table_nodes = [n for n in self.graph.nodes if isinstance(n, DATASET_CLASSES)]
        return self.graph.subgraph(table_nodes)

    @property
    def column_lineage_graph(self) -> DiGraph:
        """
        The column level DiGraph held by SQLLineageHolder
        """
        column_nodes = [n for n in self.graph.nodes if isinstance(n, Column)]
        return self.graph.subgraph(column_nodes)

    @property
    def source_tables(self) -> set[Table]:
        """
        a list of source :class:`sqllineage.core.models.Table`
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
    def target_tables(self) -> set[Table]:
        """
        a list of target :class:`sqllineage.core.models.Table`
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
    def intermediate_tables(self) -> set[Table]:
        """
        a list of intermediate :class:`sqllineage.core.models.Table`
        """
        intermediate_tables = {
            table for table, deg in self.table_lineage_graph.in_degree if deg > 0
        }.intersection(
            {table for table, deg in self.table_lineage_graph.out_degree if deg > 0}
        )
        intermediate_tables -= self.__retrieve_tag_tables(NodeTag.SELFLOOP)
        return intermediate_tables

    def __retrieve_tag_tables(self, tag) -> set[Union[Path, Table]]:
        return {
            table
            for table, attr in self.graph.nodes(data=True)
            if attr.get(tag) is True and isinstance(table, DATASET_CLASSES)
        }

    @staticmethod
    def _build_digraph(
        metadata_provider: MetaDataProvider, *args: StatementLineageHolder
    ) -> DiGraph:
        g = DiGraph()
        for holder in args:
            g = nx.compose(g, holder.graph)
            if holder.drop:
                for table in holder.drop:
                    if g.has_node(table) and g.degree[table] == 0:
                        g.remove_node(table)
            elif holder.rename:
                for table_old, table_new in holder.rename:
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
            if isinstance(s, Column) and len(s.parent_candidates) > 1
        ]
        for unresolved_col, tgt_col in unresolved_cols:
            # check if there's only one parent candidate contains the column with same name
            src_cols = []
            # check if source column exists in graph (either from subquery or from table created in prev statement)
            for parent in unresolved_col.parent_candidates:
                src_col = Column(unresolved_col.raw_name)
                src_col.parent = parent
                if g.has_edge(parent, src_col):
                    src_cols.append(src_col)
            # if not in graph, check if defined in table schema by metadata service
            if len(src_cols) == 0 and bool(metadata_provider):
                for parent in unresolved_col.parent_candidates:
                    if (
                        isinstance(parent, Table)
                        and str(parent.schema) != Schema.unknown
                    ):
                        columns = metadata_provider.get_table_columns(parent)
                        for src_col in columns:
                            if unresolved_col.raw_name == src_col.raw_name:
                                src_cols.append(src_col)

            # Multiple sources is a correct case for JOIN with USING
            # It incorrect for JOIN with ON, but sql without specifying an alias in this case will be invalid
            for src_col in src_cols:
                g.add_edge(src_col, tgt_col, type=EdgeType.LINEAGE)
            if len(src_cols) > 0:
                # only delete unresolved column when it's resolved
                g.remove_edge(unresolved_col, tgt_col)

        # when unresolved column got resolved, it will be orphan node, and we can remove it
        for node in [n for n, deg in g.degree if deg == 0]:
            if isinstance(node, Column) and len(node.parent_candidates) > 1:
                g.remove_node(node)
        return g

    @staticmethod
    def of(metadata_provider, *args: StatementLineageHolder) -> "SQLLineageHolder":
        """
        To assemble multiple :class:`sqllineage.core.holders.StatementLineageHolder` into
        :class:`sqllineage.core.holders.SQLLineageHolder`
        """
        g = SQLLineageHolder._build_digraph(metadata_provider, *args)
        return SQLLineageHolder(g)
