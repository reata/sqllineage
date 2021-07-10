from sqlparse.sql import Function, Token

from sqllineage.core.handlers.base import CurrentTokenBaseHandler
from sqllineage.core.lineage_result import LineageResult
from sqllineage.helpers import escape_identifier_name
from sqllineage.models import Table


class SwapPartitionHandler(CurrentTokenBaseHandler):
    """
    a special handling for swap_partitions_between_tables function of Vertica SQL dialect.
    """

    def handle(self, token: Token, lineage_result: LineageResult) -> None:
        if (
            isinstance(token, Function)
            and token.get_name().lower() == "swap_partitions_between_tables"
        ):
            _, parenthesis = token.tokens
            _, identifier_list, _ = parenthesis.tokens
            identifiers = list(identifier_list.get_identifiers())
            lineage_result.read.add(
                Table(escape_identifier_name(identifiers[0].normalized))
            )
            lineage_result.write.add(
                Table(escape_identifier_name(identifiers[3].normalized))
            )
