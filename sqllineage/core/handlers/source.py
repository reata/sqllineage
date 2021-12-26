import re
from typing import Dict, List, Union

from sqlparse.sql import (
    Case,
    Function,
    Identifier,
    IdentifierList,
    Operation,
    Parenthesis,
    Token,
)
from sqlparse.tokens import Literal, Wildcard

from sqllineage.core.handlers.base import NextTokenBaseHandler
from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.models import Column, Path, SubQuery, Table
from sqllineage.exceptions import SQLLineageException
from sqllineage.utils.constant import EdgeType
from sqllineage.utils.sqlparse import get_subquery_parentheses


class SourceHandler(NextTokenBaseHandler):
    """Source Table & Column Handler."""

    SOURCE_TABLE_TOKENS = (
        r"FROM",
        # inspired by https://github.com/andialbrecht/sqlparse/blob/master/sqlparse/keywords.py
        r"((LEFT\s+|RIGHT\s+|FULL\s+)?(INNER\s+|OUTER\s+|STRAIGHT\s+)?|(CROSS\s+|NATURAL\s+)?)?JOIN",
    )

    def __init__(self):
        self.column_flag = False
        self.columns = []
        self.tables = []
        self.union_barriers = []
        super().__init__()

    def _indicate(self, token: Token) -> bool:
        if token.normalized in ("UNION", "UNION ALL"):
            self.union_barriers.append((len(self.columns), len(self.tables)))

        if any(re.match(regex, token.normalized) for regex in self.SOURCE_TABLE_TOKENS):
            self.column_flag = False
            return True
        elif bool(token.normalized == "SELECT"):
            self.column_flag = True
            return True
        else:
            return False

    def _handle(self, token: Token, holder: SubQueryLineageHolder) -> None:
        if self.column_flag:
            self._handle_column(token)
        else:
            self._handle_table(token, holder)

    def _handle_table(self, token: Token, holder: SubQueryLineageHolder) -> None:
        if isinstance(token, Identifier):
            self.tables.append(self._get_dataset_from_identifier(token, holder))
        elif isinstance(token, IdentifierList):
            # This is to support join in ANSI-89 syntax
            for identifier in token.get_sublists():
                self.tables.append(
                    self._get_dataset_from_identifier(identifier, holder)
                )
        elif isinstance(token, Parenthesis):
            # SELECT col1 FROM (SELECT col2 FROM tab1), the subquery will be parsed as Parenthesis
            # This syntax without alias for subquery is invalid in MySQL, while valid for SparkSQL
            self.tables.append(SubQuery.of(token, None))
        elif token.ttype == Literal.String.Single:
            self.tables.append(Path(token.value))
        else:
            raise SQLLineageException(
                "An Identifier is expected, got %s[value: %s] instead."
                % (type(token).__name__, token)
            )

    def _handle_column(self, token: Token) -> None:
        column_token_types = (Identifier, Function, Operation, Case)
        if isinstance(token, column_token_types) or token.ttype is Wildcard:
            column_tokens = [token]
        elif isinstance(token, IdentifierList):
            column_tokens = [
                sub_token
                for sub_token in token.tokens
                if isinstance(sub_token, column_token_types)
            ]
        else:
            # SELECT constant value will end up here
            column_tokens = []
        for token in column_tokens:
            self.columns.append(Column.of(token))

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

    @classmethod
    def _get_dataset_from_identifier(
        cls, identifier: Identifier, holder: SubQueryLineageHolder
    ) -> Union[Path, Table, SubQuery]:
        path_match = re.match(r"(parquet|csv|json)\.`(.*)`", identifier.value)
        if path_match is not None:
            return Path(path_match.groups()[1])
        else:
            read: Union[Table, SubQuery, None] = None
            subqueries = get_subquery_parentheses(identifier)
            if len(subqueries) > 0:
                # SELECT col1 FROM (SELECT col2 FROM tab1) dt, the subquery will be parsed as Identifier
                # referring https://github.com/andialbrecht/sqlparse/issues/218 for further information
                parenthesis, alias = subqueries[0]
                read = SubQuery.of(parenthesis, alias)
            else:
                cte_dict = {s.alias: s for s in holder.cte}
                if "." not in identifier.value:
                    cte = cte_dict.get(identifier.get_real_name())
                    if cte is not None:
                        # could reference CTE with or without alias
                        read = SubQuery.of(
                            cte.token,
                            identifier.get_alias() or identifier.get_real_name(),
                        )
                if read is None:
                    read = Table.of(identifier)
            return read

    @classmethod
    def _get_alias_mapping_from_table_group(
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
