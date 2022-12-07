from functools import reduce
from operator import add
from typing import List, NamedTuple, Optional, Set, Union

from sqlparse.sql import (
    Function,
    Identifier,
    IdentifierList,
    Statement,
    TokenList,
    Where,
)
from sqllineage.core.models import Table

from sqllineage.core.handlers.base import (
    CurrentTokenBaseHandler,
    NextTokenBaseHandler,
)
from sqllineage.core.holders import StatementLineageHolder, SubQueryLineageHolder
from sqllineage.core.models import SubQuery
from sqllineage.utils.sqlparse import (
    get_subquery_parentheses,
    is_subquery,
    is_token_negligible,
)


class AnalyzerContext(NamedTuple):
    subquery: Optional[SubQuery] = None
    prev_cte: Optional[Set[SubQuery]] = None
    prev_write: Optional[Set[Table]] = None


class LineageAnalyzer:
    """SQL Statement Level Lineage Analyzer."""

    def analyze(self, stmt: Statement) -> StatementLineageHolder:
        """
        to analyze the Statement and store the result into :class:`sqllineage.holders.StatementLineageHolder`.

        :param stmt: a SQL statement parsed by `sqlparse`
        """
        return StatementLineageHolder.of(
            self._extract_from_dml(stmt, AnalyzerContext())
        )

    @classmethod
    def _extract_from_dml(
        cls, token: TokenList, context: AnalyzerContext
    ) -> SubQueryLineageHolder:
        holder = SubQueryLineageHolder()
        if context.prev_cte is not None:
            # CTE can be referenced by subsequent CTEs
            for cte in context.prev_cte:
                holder.add_cte(cte)
        if context.subquery is not None:
            # If within subquery, then manually add subquery as target table
            holder.add_write(context.subquery)
        current_handlers = [
            handler_cls() for handler_cls in CurrentTokenBaseHandler.__subclasses__()
        ]
        next_handlers = [
            handler_cls() for handler_cls in NextTokenBaseHandler.__subclasses__()
        ]

        subqueries = []
        tokens = [t for t in token.tokens if not is_token_negligible(t)]
        for sub_token in tokens:

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
            holder |= cls._extract_from_dml(sq.token, AnalyzerContext(sq, holder.cte))
        return holder

    @classmethod
    def parse_subquery(cls, token: TokenList) -> List[SubQuery]:
        result = []
        if isinstance(token, (Identifier, Function, Where)):
            # usually SubQuery is an Identifier, but not all Identifiers are SubQuery
            # Function for CTE without AS keyword
            result = cls._parse_subquery(token)
        elif isinstance(token, IdentifierList):
            # IdentifierList for SQL89 style of JOIN or multiple CTEs, this is actually SubQueries
            result = reduce(
                add,
                [
                    cls._parse_subquery(identifier)
                    for identifier in token.get_sublists()
                ],
                [],
            )
        elif is_subquery(token):
            # Parenthesis for SubQuery without alias, this is valid syntax for certain SQL dialect
            result = [SubQuery.of(token, None)]
        return result

    @classmethod
    def _parse_subquery(
        cls, token: Union[Identifier, Function, Where]
    ) -> List[SubQuery]:
        """
        convert SubQueryTuple to sqllineage.core.models.SubQuery
        """
        return [
            SubQuery.of(parenthesis, alias)
            for parenthesis, alias in get_subquery_parentheses(token)
        ]
