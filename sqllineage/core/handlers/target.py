from sqlparse.sql import Comparison, Function, Identifier, Token
from sqlparse.tokens import Number

from sqllineage.core.handlers.base import NextTokenBaseHandler
from sqllineage.core.lineage_result import LineageResult
from sqllineage.exceptions import SQLLineageException
from sqllineage.models import Table


class TargetHandler(NextTokenBaseHandler):
    TARGET_TABLE_TOKENS = ("INTO", "OVERWRITE", "TABLE", "VIEW", "UPDATE")

    def _indicate(self, token: Token) -> bool:
        return token.normalized in self.TARGET_TABLE_TOKENS

    def _handle(self, token: Token, lineage_result: LineageResult) -> None:
        if isinstance(token, Function):
            # insert into tab (col1, col2) values (val1, val2); Here tab (col1, col2) will be parsed as Function
            # referring https://github.com/andialbrecht/sqlparse/issues/483 for further information
            if not isinstance(token.token_first(skip_cm=True), Identifier):
                raise SQLLineageException(
                    "An Identifier is expected, got %s[value: %s] instead."
                    % (type(token).__name__, token)
                )
            lineage_result.write.add(Table.create(token.token_first(skip_cm=True)))
        elif isinstance(token, Comparison):
            # create table tab1 like tab2, tab1 like tab2 will be parsed as Comparison
            # referring https://github.com/andialbrecht/sqlparse/issues/543 for further information
            if not (
                isinstance(token.left, Identifier)
                and isinstance(token.right, Identifier)
            ):
                raise SQLLineageException(
                    "An Identifier is expected, got %s[value: %s] instead."
                    % (type(token).__name__, token)
                )
            lineage_result.write.add(Table.create(token.left))
            lineage_result.read.add(Table.create(token.right))
        else:
            if not isinstance(token, Identifier):
                raise SQLLineageException(
                    "An Identifier is expected, got %s[value: %s] instead."
                    % (type(token).__name__, token)
                )
            if token.token_first(skip_cm=True).ttype is Number.Integer:
                # Special Handling for Spark Bucket Table DDL
                pass
            else:
                lineage_result.write.add(Table.create(token))
