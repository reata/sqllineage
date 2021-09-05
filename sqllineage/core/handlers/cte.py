from sqlparse.sql import Identifier, IdentifierList, Token

from sqllineage.core.handlers.base import NextTokenBaseHandler
from sqllineage.core.lineage_result import LineageResult
from sqllineage.exceptions import SQLLineageException
from sqllineage.models import Table


class CTEHandler(NextTokenBaseHandler):
    CTE_TOKENS = ("WITH",)

    def _indicate(self, token: Token) -> bool:
        return token.normalized in self.CTE_TOKENS

    def _handle(self, token: Token, lineage_result: LineageResult, **kwargs) -> None:
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
            lineage_result.intermediate.add(Table.of(token))
