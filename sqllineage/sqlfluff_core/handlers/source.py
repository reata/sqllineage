import re
from typing import Dict, List, Union, Iterable

from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.models import Path, Table, SubQuery
from sqllineage.exceptions import SQLLineageException
from sqllineage.sqlfluff_core.handlers.base import ConditionalSegmentBaseHandler
from sqllineage.sqlfluff_core.models import SqlFluffSubQuery, SqlFluffTable
from sqllineage.sqlfluff_core.utils.sqlfluff import (
    is_subquery,
    is_values_clause,
    is_segment_negligible,
    get_subqueries,
    get_multiple_identifiers,
    get_inner_from_expression,
)
from sqllineage.utils.constant import EdgeType


class SourceHandler(ConditionalSegmentBaseHandler):
    """Source Table & Column Handler."""

    def __init__(self):
        self.columns = []
        self.tables = []
        self.union_barriers = []
        super().__init__()

    def indicate(self, segment: BaseSegment) -> bool:
        if segment.type == "from_clause":
            return True
        else:
            return False

    def handle(self, segment: BaseSegment, holder: SubQueryLineageHolder) -> None:
        self._handle_table(segment, holder)
        if self.indicate_column(segment):
            self._handle_column(segment)

    def _handle_table(
        self, initial_segment: BaseSegment, holder: SubQueryLineageHolder
    ) -> None:
        identifiers = get_multiple_identifiers(initial_segment)
        if identifiers and len(identifiers) > 1:
            for identifier in identifiers:
                self._add_dataset_from_identifier(identifier, holder)
        segment = get_inner_from_expression(initial_segment)
        if segment.type == "from_expression_element":
            self._add_dataset_from_identifier(segment, holder)
        elif segment.type == "bracketed":
            if is_subquery(segment):
                # SELECT col1 FROM (SELECT col2 FROM tab1), the subquery will be parsed as bracketed
                # This syntax without alias for subquery is invalid in MySQL, while valid for SparkSQL
                self.tables.append(SqlFluffSubQuery.of(segment, None))
        else:
            raise SQLLineageException(
                "An Identifier is expected, got %s[value: %s] instead."
                % (type(BaseSegment).__name__, BaseSegment)
            )
        for extra_segment in self._retrieve_extra_segment(initial_segment):
            self._handle_table(extra_segment, holder)

    def _handle_column(self, segment: BaseSegment) -> None:
        pass

    def end_of_query_cleanup(self, holder: SubQueryLineageHolder) -> None:
        for i, tbl in enumerate(self.tables):
            holder.add_read(tbl)
        self.union_barriers.append((len(self.columns), len(self.tables)))
        for i, (col_barrier, tbl_barrier) in enumerate(self.union_barriers):
            prev_col_barrier, prev_tbl_barrier = (
                (0, 0) if i == 0 else self.union_barriers[i - 1]
            )
            col_grp = self.columns[prev_col_barrier:col_barrier]
            tbl_grp = self.tables[prev_tbl_barrier:tbl_barrier]
            tgt_tbl = None
            if holder.write:
                if len(holder.write) > 1:
                    raise SQLLineageException
                tgt_tbl = list(holder.write)[0]
            if tgt_tbl:
                for tgt_col in col_grp:
                    tgt_col.parent = tgt_tbl
                    for src_col in tgt_col.to_source_columns(
                        self._get_alias_mapping_from_table_group(tbl_grp, holder)
                    ):
                        holder.add_column_lineage(src_col, tgt_col)

    def _add_dataset_from_identifier(
        self, identifier: BaseSegment, holder: SubQueryLineageHolder
    ) -> None:
        dataset: Union[Path, Table, SqlFluffSubQuery]
        first_token = [
            seg
            for seg in identifier.segments
            if not is_segment_negligible(seg) and seg.type != "keyword"
        ][0]
        function_as_table = (
            identifier.get_child("table_expression").get_child("function")
            if identifier.get_child("table_expression")
            else None
        )
        if first_token.type == "function" or function_as_table:
            # function() as alias, no dataset involved
            return
        elif first_token.type == "bracketed" and is_values_clause(first_token):
            # (VALUES ...) AS alias, no dataset involved
            return
        path_match = re.match(r"(parquet|csv|json)\.`(.*)`", identifier.raw_upper)
        if path_match is not None:
            dataset = Path(path_match.groups()[1])
        else:
            read: Union[Table, SqlFluffSubQuery, None] = None
            subqueries = get_subqueries(identifier)
            if subqueries:
                # SELECT col1 FROM (SELECT col2 FROM tab1) dt, the subquery will be parsed as Identifier
                # referring https://github.com/andialbrecht/sqlparse/issues/218 for further information
                parenthesis, alias = subqueries[0]
                read = SqlFluffSubQuery.of(parenthesis, alias)
            else:
                cte_dict = {s.alias: s for s in holder.cte}
                table_identifier = self._find_table_identifier(identifier)
                if "." not in table_identifier.raw:
                    cte = cte_dict.get(table_identifier.raw)
                    if cte is not None:
                        # could reference CTE with or without alias
                        read = SqlFluffSubQuery.of(
                            cte.token,
                            # identifier.get_alias() or identifier.get_real_name(),
                            table_identifier.raw,
                        )
                if read is None:
                    read = SqlFluffTable.of(table_identifier)
            dataset = read
        self.tables.append(dataset)

    @classmethod
    def _get_alias_mapping_from_table_group(
        cls,
        table_group: List[Union[Path, Table, SubQuery, SqlFluffSubQuery]],
        holder: SubQueryLineageHolder,
    ) -> Dict[str, Union[Path, Table, SubQuery, SqlFluffSubQuery]]:
        """
        A table can be referred to as alias, table name, or database_name.table_name, create the mapping here.
        For SubQuery, it's only alias then.
        """
        return {
            **{
                tgt: src
                for src, tgt, attr in holder.graph.edges(data=True)
                if attr.get("type") == EdgeType.HAS_ALIAS and src in table_group
            },
            **{
                table.raw_name: table
                for table in table_group
                if isinstance(table, Table)
            },
            **{str(table): table for table in table_group if isinstance(table, Table)},
        }

    @classmethod
    def _find_table_identifier(cls, identifier: BaseSegment) -> BaseSegment:
        table_identifier = None
        if identifier.segments:
            for segment in identifier.segments:
                if segment.type == "table_reference":
                    return segment
                else:
                    table_identifier = cls._find_table_identifier(segment)
                    if table_identifier:
                        return table_identifier
        return table_identifier

    @staticmethod
    def indicate_column(segment: BaseSegment) -> bool:
        return bool(segment.type == "select_clause")

    def _retrieve_extra_segment(self, segment: BaseSegment) -> Iterable[BaseSegment]:
        if segment.get_child("from_expression"):
            for sgmnt in segment.get_child("from_expression").segments:
                if self._is_extra_segment(sgmnt):
                    yield sgmnt

    @staticmethod
    def _is_extra_segment(segment: BaseSegment) -> bool:
        return bool(segment.type == "join_clause")
