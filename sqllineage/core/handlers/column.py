from sqlparse.sql import Case, Function, Identifier, IdentifierList, Operation, Token
from sqlparse.tokens import Wildcard

from sqllineage.core.handlers.base import NextTokenBaseHandler
from sqllineage.core.lineage_result import LineageResult
from sqllineage.exceptions import SQLLineageException
from sqllineage.models import Column


class ColumnHandler(NextTokenBaseHandler):
    def __init__(self):
        self.source_columns = []
        self.target_columns = []
        super().__init__()

    def _indicate(self, token: Token) -> bool:
        # OVER here is to handle window function like row_number()
        return token.normalized in ("SELECT", "OVER")

    def _handle(
        self, token: Token, lineage_result: LineageResult, subquery_name=None, **kwargs
    ) -> None:
        target_table = None
        if subquery_name is not None:
            target_table = subquery_name
        else:
            if lineage_result.write:
                if len(lineage_result.write) > 1:
                    raise SQLLineageException
                target_table = list(lineage_result.write)[0]
        if target_table:
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
                self.target_columns.append(Column.of(token, target_table))

    def end_of_query_cleanup(
        self, lineage_result: LineageResult, subquery_name=None, **kwargs
    ) -> None:
        for column in self.target_columns:
            source_columns = column.to_source_columns(lineage_result.read)
            for source_column in source_columns:
                self.source_columns.append(source_column)
                lineage_result.graph.add_edge(source_column, column)
