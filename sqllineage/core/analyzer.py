from functools import reduce
from operator import add
from typing import List

from sqlparse import tokens as T
from sqlparse.sql import (
    Comment,
    Identifier,
    IdentifierList,
    Parenthesis,
    Statement,
    TokenList,
)

from sqllineage.combiners import combine_subquery_lineage
from sqllineage.core.handlers.base import (
    CurrentTokenBaseHandler,
    NextTokenBaseHandler,
)
from sqllineage.holders import StatementLineageHolder, SubQueryLineageHolder
from sqllineage.models import SubQuery, Table


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
        ):
            holder = StatementLineageHolder()
        elif stmt.get_type() == "DROP":
            holder = self._extract_from_ddl_drop(stmt)
        elif stmt.get_type() == "ALTER":
            holder = self._extract_from_ddl_alter(stmt)
        else:
            # DML parsing logic also applies to CREATE DDL
            holder = self._extract_from_dml(stmt).to_stmt_lineage_holder()
        return holder

    @classmethod
    def _extract_from_ddl_drop(cls, stmt: Statement) -> StatementLineageHolder:
        holder = StatementLineageHolder()
        for table in {Table.of(t) for t in stmt.tokens if isinstance(t, Identifier)}:
            holder.drop.add(table)
        return holder

    @classmethod
    def _extract_from_ddl_alter(cls, stmt: Statement) -> StatementLineageHolder:
        holder = StatementLineageHolder()
        tables = [Table.of(t) for t in stmt.tokens if isinstance(t, Identifier)]
        keywords = [t for t in stmt.tokens if t.is_keyword]
        if any(k.normalized == "RENAME" for k in keywords) and len(tables) == 2:
            holder.rename.add((tables[0], tables[1]))
        if any(k.normalized == "EXCHANGE" for k in keywords) and len(tables) == 2:
            holder.write.add(tables[0])
            holder.read.add(tables[1])
        return holder

    @classmethod
    def _extract_from_dml(
        cls, token: TokenList, subquery_name=None
    ) -> SubQueryLineageHolder:
        holder = SubQueryLineageHolder()
        if subquery_name is not None:
            # If within subquery, then manually add subquery as target table
            holder.write.add(SubQuery.of(token, subquery_name))
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

            for subquery in cls.parse_subquery(sub_token):
                # Collecting subquery on the way, hold on parsing until last
                # so that each handler don't have to worry about what's inside subquery
                subqueries.append(subquery)

            for current_handler in current_handlers:
                current_handler.handle(sub_token, holder)

            if sub_token.is_keyword:
                for next_handler in next_handlers:
                    next_handler.indicate(sub_token)
                continue

            for next_handler in next_handlers:
                if next_handler.indicator:
                    next_handler.handle(sub_token, holder, subquery_name=subquery_name)
        else:
            # call end of query hook here as loop is over
            for next_handler in next_handlers:
                next_handler.end_of_query_cleanup(holder, subquery_name=subquery_name)
        # Extract each subquery and merge to parent lineage
        subquery_holders = [holder] + [
            cls._extract_from_dml(subquery.token, subquery.raw_name)
            for subquery in subqueries
        ]
        # In top level query, convert subquery lineage holder to a statement lineage holder
        return combine_subquery_lineage(*subquery_holders)

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
            )
            list()
        elif isinstance(token, Parenthesis) and cls._is_parenthesis_subquery(token):
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
        if isinstance(target, Parenthesis) and cls._is_parenthesis_subquery(target):
            subquery = [SubQuery.of(target, token.get_real_name())]
        return subquery

    @classmethod
    def _is_parenthesis_subquery(cls, token: Parenthesis) -> bool:
        flag = False
        _, sub_token = token.token_next_by(m=(T.DML, "SELECT"))
        if sub_token is not None:
            flag = True
        return flag
