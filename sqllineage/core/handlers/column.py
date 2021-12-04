from sqlparse.sql import Case, Function, Identifier, IdentifierList, Operation, Token
from sqlparse.tokens import Wildcard

from sqllineage.core.handlers.base import NextTokenBaseHandler
from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.models import Column
from sqllineage.exceptions import SQLLineageException


class ColumnHandler(NextTokenBaseHandler):
    """Source & Target Column Handler."""

    def __init__(self):
        self.columns = []
        super().__init__()

    def _indicate(self, token: Token) -> bool:
        return bool(token.normalized == "SELECT")

    def _handle(self, token: Token, holder: SubQueryLineageHolder) -> None:
        column_token_types = (Identifier, Function, Operation, Case)
        if isinstance(token, column_token_types) or token.ttype is Wildcard:
            column_tokens = [token]
        elif isinstance(token, IdentifierList):
            column_tokens = [
                sub_token
                for sub_token in token.tokens
                if isinstance(sub_token, column_token_types)
            ]
        else:
            # SELECT constant value will end up here
            column_tokens = []
        for token in column_tokens:
            self.columns.append(Column.of(token))

    def end_of_query_cleanup(self, holder: SubQueryLineageHolder) -> None:
        tgt_tbl = None
        if holder.write:
            if len(holder.write) > 1:
                raise SQLLineageException
            tgt_tbl = list(holder.write)[0]
        if tgt_tbl:
            for tgt_col in self.columns:
                tgt_col.parent = tgt_tbl
                for src_col in tgt_col.to_source_columns(holder.alias_mapping):
                    holder.add_column_lineage(src_col, tgt_col)
