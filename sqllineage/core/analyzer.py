from functools import reduce
from operator import add
from typing import List

from sqlparse import tokens as T
from sqlparse.sql import (
    Case,
    Comment,
    Comparison,
    Identifier,
    IdentifierList,
    Statement,
    TokenList,
)

from sqllineage.core.handlers.base import (
    CurrentTokenBaseHandler,
    NextTokenBaseHandler,
)
from sqllineage.core.holders import StatementLineageHolder, SubQueryLineageHolder
from sqllineage.core.models import SubQuery, Table
from sqllineage.utils.sqlparse import is_subquery


class LineageAnalyzer:
    """SQL Statement Level Lineage Analyzer."""

    def analyze(self, stmt: Statement) -> StatementLineageHolder:
        """
        to analyze the Statement and store the result into :class:`sqllineage.holders.StatementLineageHolder`.

        :param stmt: a SQL statement parsed by `sqlparse`
        """
        if (
            stmt.get_type() == "DELETE"
            or stmt.token_first(skip_cm=True).normalized == "TRUNCATE"
            or stmt.token_first(skip_cm=True).normalized.upper() == "REFRESH"
            or stmt.token_first(skip_cm=True).normalized == "CACHE"
            or stmt.token_first(skip_cm=True).normalized.upper() == "UNCACHE"
            or stmt.token_first(skip_cm=True).normalized == "SHOW"
        ):
            holder = StatementLineageHolder()
        elif stmt.get_type() == "DROP":
            holder = self._extract_from_ddl_drop(stmt)
        elif stmt.get_type() == "ALTER":
            holder = self._extract_from_ddl_alter(stmt)
        else:
            # DML parsing logic also applies to CREATE DDL
            holder = StatementLineageHolder.of(self._extract_from_dml(stmt))
        return holder

    @classmethod
    def _extract_from_ddl_drop(cls, stmt: Statement) -> StatementLineageHolder:
        holder = StatementLineageHolder()
        for table in {Table.of(t) for t in stmt.tokens if isinstance(t, Identifier)}:
            holder.add_drop(table)
        return holder

    @classmethod
    def _extract_from_ddl_alter(cls, stmt: Statement) -> StatementLineageHolder:
        holder = StatementLineageHolder()
        tables = [Table.of(t) for t in stmt.tokens if isinstance(t, Identifier)]
        keywords = [t for t in stmt.tokens if t.is_keyword]
        if any(k.normalized == "RENAME" for k in keywords) and len(tables) == 2:
            holder.add_rename(tables[0], tables[1])
        if any(k.normalized == "EXCHANGE" for k in keywords) and len(tables) == 2:
            holder.add_write(tables[0])
            holder.add_read(tables[1])
        return holder

    @classmethod
    def _extract_from_dml(
        cls, token: TokenList, subquery: SubQuery = None
    ) -> SubQueryLineageHolder:
        holder = SubQueryLineageHolder()
        if subquery is not None:
            # If within subquery, then manually add subquery as target table
            holder.add_write(subquery)
        current_handlers = [
            handler_cls() for handler_cls in CurrentTokenBaseHandler.__subclasses__()
        ]
        next_handlers = [
            handler_cls() for handler_cls in NextTokenBaseHandler.__subclasses__()
        ]

        subqueries = []
        for sub_token in token.tokens:
            if cls.__token_negligible_before_tablename(sub_token):
                continue

            for sq in cls.parse_subquery(sub_token):
                # Collecting subquery on the way, hold on parsing until last
                # so that each handler don't have to worry about what's inside subquery
                subqueries.append(sq)

            for current_handler in current_handlers:
                current_handler.handle(sub_token, holder)

            if sub_token.is_keyword:
                for next_handler in next_handlers:
                    next_handler.indicate(sub_token)
                continue

            for next_handler in next_handlers:
                if next_handler.indicator:
                    next_handler.handle(sub_token, holder)
        else:
            # call end of query hook here as loop is over
            for next_handler in next_handlers:
                next_handler.end_of_query_cleanup(holder)
        # By recursively extracting each subquery of the parent and merge, we're doing Depth-first search
        for sq in subqueries:
            holder |= cls._extract_from_dml(sq.token, sq)
        return holder

    @classmethod
    def __token_negligible_before_tablename(cls, token: TokenList) -> bool:
        return token.is_whitespace or isinstance(token, Comment)

    @classmethod
    def parse_subquery(cls, token: TokenList) -> List[SubQuery]:
        result = []
        if isinstance(token, Identifier):
            # usually SubQuery is an Identifier, but not all Identifiers are SubQuery
            result = cls._parse_subquery_from_identifier(token)
        elif isinstance(token, IdentifierList):
            # IdentifierList for SQL89 style of JOIN or multiple CTEs, this is actually SubQueries
            result = reduce(
                add,
                [
                    cls._parse_subquery_from_identifier(identifier)
                    for identifier in token.get_sublists()
                ],
                [],
            )
        elif is_subquery(token):
            # Parenthesis for SubQuery without alias, this is valid syntax for certain SQL dialect
            result = [SubQuery.of(token, None)]
        return result

    @classmethod
    def _parse_subquery_from_identifier(cls, token: Identifier) -> List[SubQuery]:
        """
        the returned list is either empty when no SubQuery parsed or list of one SubQuery
        """
        subquery = []
        kw_idx, kw = token.token_next_by(m=(T.Keyword, "AS"))
        sublist = list(token.get_sublists())
        if kw is not None and len(sublist) == 1:
            # CTE: tbl AS (SELECT 1)
            target = sublist[0]
        else:
            # normal subquery: (SELECT 1) tbl
            target = token.token_first(skip_cm=True)
        if isinstance(target, Case):
            # CASE WHEN (SELECT count(*) from tab1) > 0 THEN (SELECT count(*) FROM tab1) ELSE -1
            for tk in target.get_sublists():
                if isinstance(tk, Comparison):
                    if is_subquery(tk.left):
                        subquery.append(SubQuery.of(tk.left, tk.left.get_real_name()))
                    if is_subquery(tk.right):
                        subquery.append(SubQuery.of(tk.right, tk.right.get_real_name()))
                elif is_subquery(tk):
                    subquery.append(SubQuery.of(tk, token.get_real_name()))
        if is_subquery(target):
            subquery = [SubQuery.of(target, token.get_real_name())]
        return subquery
