from sqlparse.sql import Case, Function, Identifier, IdentifierList, Operation, Token
from sqlparse.tokens import Wildcard

from sqllineage.core.handlers.base import SectionTokenBaseHandler
from sqllineage.core.lineage_result import LineageResult
from sqllineage.exceptions import SQLLineageException
from sqllineage.models import Column


class ColumnHandler(SectionTokenBaseHandler):
    def __init__(self):
        self.source_columns = []
        self.target_columns = []
        super().__init__()

    def _indicate_start(self, token: Token) -> bool:
        # we need to explicit cast here or mypy evaludates to Any, see https://github.com/python/mypy/issues/5697
        # OVER here is to handle window function like row_number()
        return str(token.normalized) in ("SELECT", "OVER")

    def _handle_start(self, token: Token, lineage_result: LineageResult) -> None:
        if lineage_result.write:
            if len(lineage_result.write) > 1:
                raise SQLLineageException
            target_table = list(lineage_result.write)[0]
            column_token_types = (Identifier, Function, Operation, Case)
            if isinstance(token, column_token_types) or token.ttype is Wildcard:
                column = [token]
            elif isinstance(token, IdentifierList):
                column = [
                    sub_token
                    for sub_token in token.tokens
                    if isinstance(sub_token, column_token_types)
                ]
            else:
                raise SQLLineageException
            for token in column:
                self.target_columns.append(Column.of(token, target_table))

    def _indicate_end(self, token: Token) -> bool:
        # FIXME
        import re

        tokens = (
            r"FROM",
            # inspired by https://github.com/andialbrecht/sqlparse/blob/master/sqlparse/keywords.py
            r"((LEFT\s+|RIGHT\s+|FULL\s+)?(INNER\s+|OUTER\s+|STRAIGHT\s+)?|(CROSS\s+|NATURAL\s+)?)?JOIN",
        )
        return any(re.match(regex, token.normalized) for regex in tokens)

    def _handle_end(self, token: Token, lineage_result: LineageResult) -> None:
        for column in self.target_columns:
            for source_table in lineage_result.read:
                source_columns = column.to_source_columns(source_table)
                for source_column in source_columns:
                    self.source_columns.append(source_column)
                    combination = (source_column, column)
                    if combination not in lineage_result.column:
                        lineage_result.column.append(combination)
