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

from sqllineage.core.handlers.base import (
    CurrentTokenBaseHandler,
    NextTokenBaseHandler,
)
from sqllineage.core.lineage_result import LineageResult
from sqllineage.models import Table


class LineageAnalyzer:
    """SQL Statement Level Lineage Analyzer."""

    def __init__(self) -> None:
        self._lineage_result = LineageResult()

    def analyze(self, stmt: Statement) -> LineageResult:
        """
        to analyze the Statement and store the result into :class:`LineageResult`.

        :param stmt: a SQL statement parsed by `sqlparse`
        """
        if stmt.get_type() == "DROP":
            self._extract_from_ddl_drop(stmt)
        elif stmt.get_type() == "ALTER":
            self._extract_from_ddl_alter(stmt)
        elif (
            stmt.get_type() == "DELETE"
            or stmt.token_first(skip_cm=True).normalized == "TRUNCATE"
            or stmt.token_first(skip_cm=True).normalized.upper() == "REFRESH"
            or stmt.token_first(skip_cm=True).normalized == "CACHE"
            or stmt.token_first(skip_cm=True).normalized.upper() == "UNCACHE"
        ):
            pass
        else:
            # DML parsing logic also applies to CREATE DDL
            self._extract_from_dml(stmt)
        return self._lineage_result

    def _extract_from_ddl_drop(self, stmt: Statement) -> None:
        for table in {
            Table.create(t) for t in stmt.tokens if isinstance(t, Identifier)
        }:
            self._lineage_result.drop.add(table)

    def _extract_from_ddl_alter(self, stmt: Statement) -> None:
        tables = [Table.create(t) for t in stmt.tokens if isinstance(t, Identifier)]
        keywords = [t for t in stmt.tokens if t.is_keyword]
        if any(k.normalized == "RENAME" for k in keywords) and len(tables) == 2:
            self._lineage_result.rename.add((tables[0], tables[1]))
        if any(k.normalized == "EXCHANGE" for k in keywords) and len(tables) == 2:
            self._lineage_result.write.add(tables[0])
            self._lineage_result.read.add(tables[1])

    def _extract_from_dml(self, token: TokenList) -> None:
        current_handlers = [
            handler_cls() for handler_cls in CurrentTokenBaseHandler.__subclasses__()
        ]
        next_handlers = [
            handler_cls() for handler_cls in NextTokenBaseHandler.__subclasses__()
        ]
        for sub_token in token.tokens:
            if self.__token_negligible_before_tablename(sub_token):
                continue

            for subquery in self.parse_subquery(sub_token):
                self._extract_from_dml(subquery)

            for current_handler in current_handlers:
                current_handler.handle(sub_token, self._lineage_result)

            if sub_token.is_keyword:
                for next_handler in next_handlers:
                    next_handler.indicate(sub_token)
                continue

            for next_handler in next_handlers:
                if next_handler.indicator:
                    next_handler.handle(sub_token, self._lineage_result)
        else:
            for next_handler in next_handlers:
                next_handler.end_of_query_cleanup(self._lineage_result)

    @classmethod
    def __token_negligible_before_tablename(cls, token: TokenList) -> bool:
        return token.is_whitespace or isinstance(token, Comment)

    @classmethod
    def parse_subquery(cls, token: TokenList) -> List[Parenthesis]:
        result = []
        if isinstance(token, Identifier):
            # usually SubQuery is an Identifier, but not all Identifiers are SubQuery
            result = cls._parse_subquery_from_identifier(token)
        elif isinstance(token, IdentifierList):
            # IdentifierList for SQL89 style of JOIN or multiple CTEs, this is actually SubQueries
            result = reduce(
                add,
                (
                    cls._parse_subquery_from_identifier(identifier)
                    for identifier in token.get_sublists()
                ),
            )
        elif isinstance(token, Parenthesis):
            # Parenthesis for SubQuery without alias, this is valid syntax for certain SQL dialect
            result = cls._parse_subquery_from_parenthesis(token)
        return result

    @classmethod
    def _parse_subquery_from_identifier(cls, token: Identifier) -> List[Parenthesis]:
        result = []
        kw_idx, kw = token.token_next_by(m=(T.Keyword, "AS"))
        sublist = list(token.get_sublists())
        if kw is not None and len(sublist) == 1:
            # CTE: tbl AS (SELECT 1)
            target = sublist[0]
        else:
            # normal subquery: (SELECT 1) tbl
            target = token.token_first(skip_cm=True)
        if isinstance(target, Parenthesis):
            result = cls._parse_subquery_from_parenthesis(target)
        return result

    @classmethod
    def _parse_subquery_from_parenthesis(cls, token: Parenthesis) -> List[Parenthesis]:
        result = []
        _, sub_token = token.token_next_by(m=(T.DML, "SELECT"))
        if sub_token is not None:
            result = [token]
        return result
