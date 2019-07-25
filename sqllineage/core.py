import argparse
import sys
from typing import List, Set

import sqlparse
from sqlparse.sql import Function, Identifier, IdentifierList, Statement, TokenList
from sqlparse.tokens import Keyword, Token, Whitespace

SOURCE_TABLE_TOKENS = ('FROM', 'JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'LEFT OUTER JOIN', 'RIGHT OUTER JOIN',
                       'FULL OUTER JOIN', 'CROSS JOIN')
TARGET_TABLE_TOKENS = ('INTO', 'OVERWRITE', 'TABLE')
TEMP_TABLE_TOKENS = ('WITH', )


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

    def __str__(self):
        return """Statements(#): {stmt_cnt}
Source Tables:
    {source_tables}
Target Tables:
    {target_tables}
""".format(stmt_cnt=len(self.statements), source_tables="\n    ".join(self.source_tables),
           target_tables="\n    ".join(self.target_tables))

    @property
    def statements_parsed(self) -> List[Statement]:
        return self._stmt

    @property
    def statements(self) -> List[str]:
        return [sqlparse.format(s.value) for s in self.statements_parsed]

    @property
    def source_tables(self) -> Set[str]:
        return self._source_tables

    @property
    def target_tables(self) -> Set[str]:
        return self._target_tables

    def _extract_from_token(self, token: Token):
        if not isinstance(token, TokenList):
            return
        source_table_token_flag = target_table_token_flag = temp_table_token_flag = False
        for sub_token in token.tokens:
            if isinstance(token, TokenList) and not isinstance(sub_token, (Identifier, IdentifierList)):
                self._extract_from_token(sub_token)
            if sub_token.ttype in Keyword:
                if sub_token.normalized in SOURCE_TABLE_TOKENS:
                    source_table_token_flag = True
                elif sub_token.normalized in TARGET_TABLE_TOKENS:
                    target_table_token_flag = True
                elif sub_token.normalized in TEMP_TABLE_TOKENS:
                    temp_table_token_flag = True
                continue
            elif isinstance(sub_token, Identifier) and sub_token.normalized == "OVERWRITE" \
                    and sub_token.get_alias() is not None:
                # overwrite can't be parsed as Keyword, manual walk around
                self._target_tables.add(sub_token.get_alias())
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
                elif isinstance(sub_token, Function):
                    # insert into tab (col1, col2), tab (col1, col2) will be parsed as Function
                    # referring https://github.com/andialbrecht/sqlparse/issues/483 for further information
                    assert isinstance(sub_token.token_first(), Identifier)
                    self._target_tables.add(sub_token.token_first().get_real_name())
                    target_table_token_flag = False
                else:
                    assert isinstance(sub_token, Identifier)
                    self._target_tables.add(sub_token.get_real_name())
                    target_table_token_flag = False
            elif temp_table_token_flag:
                if sub_token.ttype == Whitespace:
                    continue
                else:
                    assert isinstance(sub_token, Identifier)
                    self._source_tables.add(sub_token.get_real_name())
                    self._target_tables.add(sub_token.get_real_name())
                    self._extract_from_token(sub_token)
                    temp_table_token_flag = False


def main():
    parser = argparse.ArgumentParser(prog='sqllineage', description='SQL Lineage Parser.')
    parser.add_argument('-e', metavar='<quoted-query-string>', help='SQL from command line')
    parser.add_argument('-f', metavar='<filename>', help='SQL from files')
    args = parser.parse_args()
    if args.e and args.f:
        print("WARNING: Both -e and -f options are specified. -e option will be ignored", file=sys.stderr)
    if args.f:
        try:
            with open(args.f) as f:
                sql = f.read()
            print(LineageParser(sql))
        except FileNotFoundError:
            print("ERROR: No such file: {}".format(args.f), file=sys.stderr)
        except PermissionError:
            print("ERROR: Permission denied when reading file '{}'".format(args.f), file=sys.stderr)
    elif args.e:
        print(LineageParser(args.e))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
