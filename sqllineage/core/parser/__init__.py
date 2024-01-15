from typing import List, Tuple, Union

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.models import Column, Path, SubQuery, Table
from sqllineage.exceptions import SQLLineageException


class SourceHandlerMixin:
    tables: List[Union[Path, SubQuery, Table]]
    columns: List[Column]
    union_barriers: List[Tuple[int, int]]

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
            if holder.write:
                if len(holder.write) > 1:
                    raise SQLLineageException
                tgt_tbl = list(holder.write)[0]
                lateral_aliases = set()
                for idx, tgt_col in enumerate(col_grp):
                    tgt_col.parent = tgt_tbl
                    for lateral_alias_ref in col_grp[idx + 1 :]:  # noqa: E203
                        if any(
                            src_col[0] == tgt_col.raw_name
                            for src_col in lateral_alias_ref.source_columns
                        ):
                            lateral_aliases.add(tgt_col.raw_name)
                            break
                    for src_col in tgt_col.to_source_columns(
                        holder.get_alias_mapping_from_table_group(tbl_grp)
                    ):
                        if len(write_columns := holder.write_columns) == len(col_grp):
                            # example query: create view test (col3) select col1 as col2 from tab
                            # without write_columns = [col3] information, by default src_col = col1 and tgt_col = col2
                            # when write_columns exist and length matches, we want tgt_col = col3 instead of col2
                            # for invalid query: create view test (col3, col4) select col1 as col2 from tab,
                            # when the length doesn't match, we fall back to default behavior
                            tgt_col = write_columns[idx]
                        is_lateral_alias_ref = False
                        for wc in holder.write_columns:
                            if wc.raw_name == "*":
                                continue
                            if (
                                src_col.raw_name == wc.raw_name
                                and src_col.raw_name in lateral_aliases
                            ):
                                is_lateral_alias_ref = True
                                for lateral_alias_col in holder.get_source_columns(wc):
                                    holder.add_column_lineage(
                                        lateral_alias_col, tgt_col
                                    )
                                break
                        if is_lateral_alias_ref:
                            continue
                        holder.add_column_lineage(src_col, tgt_col)
