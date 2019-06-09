from typing import List, Set

import sqlparse
from sqlparse.sql import Identifier, IdentifierList, TokenList, Statement
from sqlparse.tokens import Keyword, Token, Whitespace

SOURCE_TABLE_TOKENS = ('FROM', 'JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'LEFT OUTER JOIN', 'RIGHT OUTER JOIN',
                       'FULL OUTER JOIN')
TARGET_TABLE_TOKENS = ('INSERT INTO', 'INSERT OVERWRITE')


class LineageParser(object):

    def __init__(self, sql: str, encoding=None):
        self._encoding = encoding
        self._source_tables = set()
        self._target_tables = set()
        self._stmt = sqlparse.parse(sql, self._encoding)
        for stmt in self._stmt:
            self._extract_from_token(stmt)
        self._tmp_tables = self._source_tables.intersection(self._target_tables)
        self._source_tables -= self._tmp_tables
        self._target_tables -= self._tmp_tables

    @property
    def statements_parsed(self) -> List[Statement]:
        return self._stmt

    @property
    def statements(self) -> List[str]:
        return [sqlparse.format(s.value) for s in self._stmt]

    @property
    def source_tables(self) -> Set[str]:
        return self._source_tables

    @property
    def target_tables(self) -> Set[str]:
        return self._target_tables

    def _extract_from_token(self, token: Token):
        if not isinstance(token, TokenList):
            return
        source_table_token_flag = target_table_token_flag = False
        for sub_token in token.tokens:
            if isinstance(token, TokenList) and not isinstance(sub_token, (Identifier, IdentifierList)):
                self._extract_from_token(sub_token)
            if sub_token.ttype in Keyword:
                if sub_token.normalized in SOURCE_TABLE_TOKENS:
                    source_table_token_flag = True
                elif sub_token.normalized in TARGET_TABLE_TOKENS:
                    target_table_token_flag = True
                continue
            if source_table_token_flag:
                if sub_token.ttype == Whitespace:
                    continue
                else:
                    assert isinstance(sub_token, Identifier)
                    self._source_tables.add(sub_token.get_real_name())
                    source_table_token_flag = False
            elif target_table_token_flag:
                if sub_token.ttype == Whitespace:
                    continue
                else:
                    assert isinstance(sub_token, Identifier)
                    self._target_tables.add(sub_token.get_real_name())
                    target_table_token_flag = False
