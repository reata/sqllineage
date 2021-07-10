from sqlparse.sql import Comment, Identifier, Statement, TokenList

from sqllineage.core.handlers.base import CurrentTokenBaseHandler, NextTokenBaseHandler
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

            if isinstance(sub_token, TokenList):
                self._extract_from_dml(sub_token)

            for current_handler in current_handlers:
                current_handler.handle(sub_token, self._lineage_result)

            if sub_token.is_keyword:
                for next_handler in next_handlers:
                    next_handler.indicate(sub_token)
                continue

            for next_handler in next_handlers:
                if next_handler.indicator:
                    next_handler.handle(sub_token, self._lineage_result)
                    if next_handler.RE_EXTRACT:
                        self._extract_from_dml(sub_token)

    @classmethod
    def __token_negligible_before_tablename(cls, token: TokenList) -> bool:
        return token.is_whitespace or isinstance(token, Comment)
