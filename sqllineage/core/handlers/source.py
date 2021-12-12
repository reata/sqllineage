import re
from typing import Union

from sqlparse.sql import Identifier, IdentifierList, Parenthesis, Token
from sqlparse.tokens import Literal

from sqllineage.core.handlers.base import NextTokenBaseHandler
from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.models import Path, SubQuery, Table
from sqllineage.exceptions import SQLLineageException
from sqllineage.utils.sqlparse import get_subquery_parentheses


class SourceHandler(NextTokenBaseHandler):
    """Source Table Handler."""

    SOURCE_TABLE_TOKENS = (
        r"FROM",
        # inspired by https://github.com/andialbrecht/sqlparse/blob/master/sqlparse/keywords.py
        r"((LEFT\s+|RIGHT\s+|FULL\s+)?(INNER\s+|OUTER\s+|STRAIGHT\s+)?|(CROSS\s+|NATURAL\s+)?)?JOIN",
    )

    def _indicate(self, token: Token) -> bool:
        return any(
            re.match(regex, token.normalized) for regex in self.SOURCE_TABLE_TOKENS
        )

    def _handle(self, token: Token, holder: SubQueryLineageHolder) -> None:
        if isinstance(token, Identifier):
            self._add_identifier_to_read(token, holder)
        elif isinstance(token, IdentifierList):
            # This is to support join in ANSI-89 syntax
            for identifier in token.get_sublists():
                self._add_identifier_to_read(identifier, holder)
        elif isinstance(token, Parenthesis):
            # SELECT col1 FROM (SELECT col2 FROM tab1), the subquery will be parsed as Parenthesis
            # This syntax without alias for subquery is invalid in MySQL, while valid for SparkSQL
            holder.add_read(SubQuery.of(token, None))
        elif token.ttype == Literal.String.Single:
            holder.add_read(Path(token.value))
        else:
            raise SQLLineageException(
                "An Identifier is expected, got %s[value: %s] instead."
                % (type(token).__name__, token)
            )

    @classmethod
    def _add_identifier_to_read(
        cls, identifier: Identifier, holder: SubQueryLineageHolder
    ):
        path_match = re.match(r"(parquet|csv|json)\.`(.*)`", identifier.value)
        if path_match is not None:
            holder.add_read(Path(path_match.groups()[1]))
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
            holder.add_read(read)
