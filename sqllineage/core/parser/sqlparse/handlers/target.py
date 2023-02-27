from sqlparse.sql import Comparison, Function, Identifier, Token
from sqlparse.tokens import Literal, Number

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.models import Path
from sqllineage.core.parser.sqlparse.handlers.base import NextTokenBaseHandler
from sqllineage.core.parser.sqlparse.models import SqlParseTable
from sqllineage.exceptions import SQLLineageException


class TargetHandler(NextTokenBaseHandler):
    """Target Table Handler."""

    TARGET_TABLE_TOKENS = (
        "INTO",
        "OVERWRITE",
        "TABLE",
        "VIEW",
        "UPDATE",
        "COPY",
        "DIRECTORY",
    )

    def _indicate(self, token: Token) -> bool:
        if (
            self.indicator is True
            and token.is_keyword
            and token.normalized in ("IF", "NOT", "EXISTS")
        ):
            # special handling for CREATE TABLE IF NOT EXISTS
            return True
        else:
            return token.normalized in self.TARGET_TABLE_TOKENS

    def _handle(self, token: Token, holder: SubQueryLineageHolder) -> None:
        if isinstance(token, Function):
            # insert into tab (col1, col2) values (val1, val2); Here tab (col1, col2) will be parsed as Function
            # referring https://github.com/andialbrecht/sqlparse/issues/483 for further information
            if not isinstance(token.token_first(skip_cm=True), Identifier):
                raise SQLLineageException(
                    "An Identifier is expected, got %s[value: %s] instead."
                    % (type(token).__name__, token)
                )
            holder.add_write(SqlParseTable.of(token.token_first(skip_cm=True)))
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
            holder.add_write(SqlParseTable.of(token.left))
            holder.add_read(SqlParseTable.of(token.right))
        elif token.ttype == Literal.String.Single:
            holder.add_write(Path(token.value))
        elif isinstance(token, Identifier):
            if token.token_first(skip_cm=True).ttype is Number.Integer:
                # Special Handling for Spark Bucket Table DDL
                pass
            else:
                holder.add_write(SqlParseTable.of(token))
