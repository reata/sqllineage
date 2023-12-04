from typing import Dict, List, Tuple, Union

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.models import Column, Path, SubQuery, Table
from sqllineage.exceptions import SQLLineageException
from sqllineage.utils.constant import EdgeType


class SourceHandlerMixin:
    tables: List[Union[Path, SubQuery, Table]]
    columns: List[Column]
    union_barriers: List[Tuple[int, int]]

    def end_of_query_cleanup(self, holder: SubQueryLineageHolder) -> None:
        subqueries_within_set = (
            self.subqueries_within_set if hasattr(self, "subqueries_within_set") else []
        )
        for i, tbl in enumerate(self.tables):
            if tbl in subqueries_within_set:
                continue
            holder.add_read(tbl)
        self.union_barriers.append((len(self.columns), len(self.tables)))
        for i, (col_barrier, tbl_barrier) in enumerate(self.union_barriers):
            prev_col_barrier, prev_tbl_barrier = (
                (0, 0) if i == 0 else self.union_barriers[i - 1]
            )
            col_grp = self.columns[prev_col_barrier:col_barrier]
            tbl_grp = self.tables[prev_tbl_barrier:tbl_barrier]

            for tg in tbl_grp:
                if tg in subqueries_within_set:
                    origin_alias = tg.alias  # type: ignore
                    tg.alias = f"{tg.alias}_{subqueries_within_set.index(tg) + 1}"  # type: ignore
                    holder.add_read(tg)
                    for cg in col_grp:
                        for j, (sc_column, sc_qulifier) in enumerate(cg.source_columns):
                            if sc_qulifier == origin_alias:
                                cg.source_columns.remove((sc_column, sc_qulifier))
                                cg.source_columns.insert(j, (sc_column, tg.alias))  # type: ignore

            if holder.write:
                if len(holder.write) > 1:
                    raise SQLLineageException
                tgt_tbl = list(holder.write)[0]
                for idx in range(len(col_grp)):
                    tgt_col = col_grp[idx]
                    tgt_col.parent = tgt_tbl
                    for src_col in tgt_col.to_source_columns(
                        self.get_alias_mapping_from_table_group(tbl_grp, holder)
                    ):
                        if len(write_columns := holder.write_columns) == len(col_grp):
                            # example query: create view test (col3) select col1 as col2 from tab
                            # without write_columns = [col3] information, by default src_col = col1 and tgt_col = col2
                            # when write_columns exist and length matches, we want tgt_col = col3 instead of col2
                            # for invalid query: create view test (col3, col4) select col1 as col2 from tab,
                            # when the length doesn't match, we fall back to default behavior
                            tgt_col = write_columns[idx]
                        holder.add_column_lineage(src_col, tgt_col)

    @classmethod
    def get_alias_mapping_from_table_group(
        cls,
        table_group: List[Union[Path, Table, SubQuery]],
        holder: SubQueryLineageHolder,
    ) -> Dict[str, Union[Path, Table, SubQuery]]:
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
