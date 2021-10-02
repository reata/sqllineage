from sqlparse.sql import Identifier, IdentifierList, Token

from sqllineage.core.handlers.base import NextTokenBaseHandler
from sqllineage.exceptions import SQLLineageException
from sqllineage.holders import SubQueryLineageHolder
from sqllineage.models import Table


class CTEHandler(NextTokenBaseHandler):
    CTE_TOKENS = ("WITH",)

    def _indicate(self, token: Token) -> bool:
        return token.normalized in self.CTE_TOKENS

    def _handle(self, token: Token, holder: SubQueryLineageHolder, **kwargs) -> None:
        if isinstance(token, Identifier):
            cte = [token]
        elif isinstance(token, IdentifierList):
            cte = [token for token in token.tokens if isinstance(token, Identifier)]
        else:
            raise SQLLineageException(
                "An Identifier or IdentifierList is expected, got %s[value: %s] instead."
                % (type(token).__name__, token)
            )
        for token in cte:
            holder.intermediate.add(Table.of(token))
