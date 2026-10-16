from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.metadata_provider import MetaDataProvider
from sqllineage.core.models import Column, Path, SubQuery, Table
from sqllineage.core.parser import SourceHandlerMixin
from sqllineage.core.parser.sqlfluff.extractors.base import BaseExtractor
from sqllineage.core.parser.sqlfluff.models import (
    SqlFluffColumn,
    SqlFluffSubQuery,
    SqlFluffTable,
)
from sqllineage.core.parser.sqlfluff.utils import (
    extract_lateral_view_aliases,
    find_table_identifier,
    is_set_expression,
    list_child_segments,
    list_lateral_view_clauses,
)
from sqllineage.utils.constant import EdgeDirection, EdgeType
from sqllineage.utils.entities import AnalyzerContext
from sqllineage.utils.helpers import escape_identifier_name


class SelectExtractor(BaseExtractor, SourceHandlerMixin):
    """
    Select statement lineage extractor
    """

    SUPPORTED_STMT_TYPES = ["select_statement", "set_expression", "bracketed"]

    def __init__(self, dialect: str, metadata_provider: MetaDataProvider):
        super().__init__(dialect, metadata_provider)
        self.columns = []
        self.tables = []
        self.union_barriers = []
        # (datasets before lateral view, lateral view, generator function, lateral view columns)
        self.lateral_views: list[
            tuple[list[Path | SubQuery | Table], SubQuery, Column, list[Column]]
        ] = []

    def extract(
        self,
        statement: BaseSegment,
        context: AnalyzerContext,
    ) -> SubQueryLineageHolder:
        holder = self._init_holder(context)
        subqueries = []
        segments = (
            [statement]
            if statement.type == "set_expression"
            else list_child_segments(statement)
        )
        for segment in segments:
            for sq in self.list_subquery(segment):
                # Collecting subquery on the way, hold on parsing until last
                # so that each handler don't have to worry about what's inside subquery
                subqueries.append(sq)

            if is_set_expression(segment):
                subqueries.extend(self._collect_subqueries_in_set_expression(segment))

        self.extract_subquery(subqueries, holder)

        for segment in segments:
            self._handle_select_statement_child_segments(segment, holder)

            if is_set_expression(segment):
                self._handle_set_expression(segment, holder)

        self.end_of_query_cleanup(holder)

        self._handle_lateral_view_column_lineage(holder)

        # CTE queries are extracted separately, so a CTE's columns aren't known
        # here. CteExtractor expands wildcards selected from a CTE once they are.
        holder.expand_wildcard(self.metadata_provider, resolve_cte=False)

        return holder

    def _handle_select_statement_child_segments(
        self, segment: BaseSegment, holder: SubQueryLineageHolder
    ):
        self._handle_swap_partition(segment, holder)
        self._handle_select_into(segment, holder)
        self.tables.extend(
            self._list_table_from_from_clause_or_join_clause(segment, holder)
        )
        self._handle_lateral_view(segment)
        self._handle_column(segment)

    def _handle_swap_partition(
        self, segment: BaseSegment, holder: SubQueryLineageHolder
    ):
        """
        A handler for swap_partitions_between_tables function supported by vertica
        """
        if self.dialect == "vertica" and segment.type == "select_clause":
            if select_clause_element := segment.get_child("select_clause_element"):
                if function := select_clause_element.get_child("function"):
                    if function_name := function.get_child("function_name"):
                        if function_name.raw_upper == "SWAP_PARTITIONS_BETWEEN_TABLES":
                            if function_contents := function.get_child(
                                "function_contents"
                            ):
                                if bracketed := function_contents.get_child(
                                    "bracketed"
                                ):
                                    expressions = bracketed.get_children("expression")
                                    holder.add_read(
                                        SqlFluffTable(
                                            escape_identifier_name(expressions[0].raw)
                                        )
                                    )
                                    holder.add_write(
                                        SqlFluffTable(
                                            escape_identifier_name(expressions[3].raw)
                                        )
                                    )

    def _handle_select_into(self, segment: BaseSegment, holder: SubQueryLineageHolder):
        """
        A handler for SELECT INTO statement supported by some dialects
        """
        if segment.type in ["into_table_clause", "into_clause"]:
            if identifier := find_table_identifier(segment):
                if table := self.find_table(identifier):
                    holder.add_write(table)

    def _handle_lateral_view(self, segment: BaseSegment) -> None:
        """
        A handler for LATERAL VIEW clause supported by hive and sparksql:
            FROM tab LATERAL VIEW explode(tab.col1) alias AS col2
        The lateral view is read like a subquery named alias, whose column col2 is generated from tab.col1
        """
        if segment.type == "from_clause":
            for from_expression in segment.get_children("from_expression"):
                for lateral_view_clause in list_lateral_view_clauses(from_expression):
                    if function := lateral_view_clause.get_child("function"):
                        alias, column_aliases = extract_lateral_view_aliases(
                            lateral_view_clause
                        )
                        lateral_view = SqlFluffSubQuery.of(lateral_view_clause, alias)
                        columns = []
                        for column_alias in column_aliases:
                            column = Column(column_alias)
                            column.parent = lateral_view
                            columns.append(column)
                        # the generator function can refer to datasets before it in the same SELECT
                        start = self.union_barriers[-1][1] if self.union_barriers else 0
                        self.lateral_views.append(
                            (
                                self.tables[start:],
                                lateral_view,
                                SqlFluffColumn.of(function),
                                columns,
                            )
                        )
                        self.tables.append(lateral_view)

    def _handle_lateral_view_column_lineage(
        self, holder: SubQueryLineageHolder
    ) -> None:
        lateral_view_column_names: dict[Path | SubQuery | Table, set[str]] = {}
        for tables, lateral_view, generator, columns in self.lateral_views:
            alias_mapping = holder.get_alias_mapping_from_table_group(tables)
            # without column alias, the generated column can still be referred to with the default name, like t.col
            generated_columns = columns + [
                e.target
                for e in holder.go.retrieve_edges_by_vertex(
                    lateral_view, EdgeDirection.OUT, EdgeType.HAS_COLUMN
                )
                if e.target not in columns
            ]
            for src_col in generator.to_source_columns(alias_mapping):
                for column in generated_columns:
                    holder.add_column_lineage(src_col, column)
            if columns:
                self._expand_lateral_view_wildcard(holder, lateral_view, columns)
            lateral_view_column_names[lateral_view] = {c.raw_name for c in columns}
        if lateral_view_column_names:
            self._bind_unqualified_column_with_lateral_view(
                holder, lateral_view_column_names
            )

    @staticmethod
    def _bind_unqualified_column_with_lateral_view(
        holder: SubQueryLineageHolder,
        lateral_view_column_names: dict[Path | SubQuery | Table, set[str]],
    ) -> None:
        """
        An unqualified column could come from any dataset in FROM clause, but the columns of a lateral view are known.
        Bind the column to the lateral view having it, otherwise rule out the lateral views.
        """
        for src_col in {
            e.source
            for e in holder.go.retrieve_edges_by_label(EdgeType.LINEAGE)
            if isinstance(e.source, Column) and len(e.source.parent_candidates) > 1
        }:
            candidates = src_col.parent_candidates
            if not any(p in lateral_view_column_names for p in candidates):
                continue
            parents = [
                p
                for p in candidates
                if src_col.raw_name in lateral_view_column_names.get(p, set())
            ] or [p for p in candidates if p not in lateral_view_column_names]
            if parents:
                tgt_cols = [
                    e.target
                    for e in holder.go.retrieve_edges_by_vertex(
                        src_col, EdgeDirection.OUT, EdgeType.LINEAGE
                    )
                ]
                # a column with multiple parent candidates is identified by name only, so replace the vertex
                holder.go.drop_vertices(src_col)
                new_src_col = Column(src_col.raw_name, escaped=True)
                for parent in parents:
                    new_src_col.parent = parent
                for tgt_col in tgt_cols:
                    holder.add_column_lineage(new_src_col, tgt_col)

    @staticmethod
    def _expand_lateral_view_wildcard(
        holder: SubQueryLineageHolder,
        lateral_view: SubQuery,
        columns: list[Column],
    ) -> None:
        """
        The columns of a lateral view are known from its column aliases, SELECT * reads each of them by name
        """
        for wildcard in [
            e.target
            for e in holder.go.retrieve_edges_by_vertex(
                lateral_view, EdgeDirection.OUT, EdgeType.HAS_COLUMN
            )
            if isinstance(e.target, Column) and e.target.raw_name == "*"
        ]:
            for tgt_wildcard in [
                e.target
                for e in holder.go.retrieve_edges_by_vertex(
                    wildcard, EdgeDirection.OUT, EdgeType.LINEAGE
                )
            ]:
                if (tgt_parent := tgt_wildcard.parent) is not None:
                    for column in columns:
                        tgt_col = Column(column.raw_name, escaped=True)
                        tgt_col.parent = tgt_parent
                        holder.add_column_lineage(column, tgt_col)
            holder.go.drop_vertices(wildcard)

    def _handle_set_expression(
        self, segment: BaseSegment, holder: SubQueryLineageHolder
    ) -> None:
        # Recursively handle set_expression and nested bracketed set_expressions
        for idx, child in enumerate(
            segment.get_children("select_statement", "bracketed")
        ):
            if idx != 0:
                self.union_barriers.append((len(self.columns), len(self.tables)))
            match child.type:
                case "select_statement":
                    for seg in list_child_segments(child):
                        self._handle_select_statement_child_segments(seg, holder)
                case "bracketed":
                    # If the bracketed child contains another set_expression, recurse; otherwise handle its contents
                    inner_children = list_child_segments(child)
                    if any(c.type == "set_expression" for c in inner_children):
                        for c in inner_children:
                            if c.type == "set_expression":
                                self._handle_set_expression(c, holder)
                    else:
                        for seg in inner_children:
                            self._handle_select_statement_child_segments(seg, holder)

    def _collect_subqueries_in_set_expression(self, segment: BaseSegment):
        subqueries = []
        for child in segment.get_children("select_statement", "bracketed"):
            match child.type:
                case "select_statement":
                    for seg in list_child_segments(child):
                        subqueries.extend(self.list_subquery(seg))
                case "bracketed":
                    inner_children = list_child_segments(child)
                    for c in inner_children:
                        if c.type == "set_expression":
                            subqueries.extend(
                                self._collect_subqueries_in_set_expression(c)
                            )
                        else:
                            subqueries.extend(self.list_subquery(c))
        return subqueries

    def _handle_column(self, segment: BaseSegment) -> None:
        """
        Column handler method
        """
        if segment.type == "select_clause":
            for sub_segment in segment.get_children("select_clause_element"):
                self.columns.append(SqlFluffColumn.of(sub_segment))
