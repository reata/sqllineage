from sqlparse.sql import Function, Token

from sqllineage.core.handlers.base import CurrentTokenBaseHandler
from sqllineage.helpers import escape_identifier_name
from sqllineage.holders import SubQueryLineageHolder
from sqllineage.models import Table


class SwapPartitionHandler(CurrentTokenBaseHandler):
    """
    a special handling for swap_partitions_between_tables function of Vertica SQL dialect.
    """

    def handle(self, token: Token, holder: SubQueryLineageHolder) -> None:
        if (
            isinstance(token, Function)
            and token.get_name().lower() == "swap_partitions_between_tables"
        ):
            _, parenthesis = token.tokens
            _, identifier_list, _ = parenthesis.tokens
            identifiers = list(identifier_list.get_identifiers())
            holder.add_read(Table(escape_identifier_name(identifiers[0].normalized)))
            holder.add_write(Table(escape_identifier_name(identifiers[3].normalized)))
