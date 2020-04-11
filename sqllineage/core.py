import argparse
import re
import sys
from typing import List, Set

import sqlparse
from sqlparse.sql import Comment, Comparison, Function, Identifier, Parenthesis, Statement, TokenList
from sqlparse.tokens import Keyword, Token

SOURCE_TABLE_TOKENS = (r'FROM',
                       # inspired by https://github.com/andialbrecht/sqlparse/blob/master/sqlparse/keywords.py
                       r'((LEFT\s+|RIGHT\s+|FULL\s+)?(INNER\s+|OUTER\s+|STRAIGHT\s+)?|(CROSS\s+|NATURAL\s+)?)?JOIN')
TARGET_TABLE_TOKENS = ('INTO', 'OVERWRITE', 'TABLE')
TEMP_TABLE_TOKENS = ('WITH',)


class LineageParser(object):

    def __init__(self, sql: str, encoding=None):
        self._encoding = encoding
        self._source_tables = set()
        self._target_tables = set()
        self._stmt = [s for s in sqlparse.parse(sql.strip(), self._encoding) if s.token_first(skip_cm=True)]
        for stmt in self._stmt:
            if stmt.get_type() == "DROP":
                self._extract_from_DDL_DROP(stmt)
            elif stmt.get_type() == "ALTER":
                self._extract_from_DDL_ALTER(stmt)
            elif stmt.get_type() == "DELETE" or stmt.token_first(skip_cm=True).normalized == "TRUNCATE":
                pass
            else:
                # DML parsing logic also applies to CREATE DDL
                self._extract_from_DML(stmt)
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
        return {t.lower() for t in self._source_tables}

    @property
    def target_tables(self) -> Set[str]:
        return {t.lower() for t in self._target_tables}

    def _extract_from_DML(self, token: Token) -> None:
        source_table_token_flag = target_table_token_flag = temp_table_token_flag = False
        for sub_token in token.tokens:
            if isinstance(sub_token, TokenList):
                self._extract_from_DML(sub_token)
            if sub_token.ttype in Keyword:
                if any(re.match(regex, sub_token.normalized) for regex in SOURCE_TABLE_TOKENS):
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
                if self.__token_negligible_before_tablename(sub_token):
                    continue
                else:
                    assert isinstance(sub_token, Identifier)
                    if isinstance(sub_token.token_first(skip_cm=True), Parenthesis):
                        # SELECT col1 FROM (SELECT col2 FROM tab1) dt, the subquery will be parsed as Identifier
                        # and this Identifier's get_real_name method would return alias name dt
                        # referring https://github.com/andialbrecht/sqlparse/issues/218 for further information
                        pass
                    else:
                        self._source_tables.add(sub_token.get_real_name())
                    source_table_token_flag = False
            elif target_table_token_flag:
                if self.__token_negligible_before_tablename(sub_token):
                    continue
                elif isinstance(sub_token, Function):
                    # insert into tab (col1, col2), tab (col1, col2) will be parsed as Function
                    # referring https://github.com/andialbrecht/sqlparse/issues/483 for further information
                    assert isinstance(sub_token.token_first(skip_cm=True), Identifier)
                    self._target_tables.add(sub_token.token_first(skip_cm=True).get_real_name())
                elif isinstance(sub_token, Comparison):
                    # create table tab1 like tab2, tab1 like tab2 will be parsed as Comparison
                    # referring https://github.com/andialbrecht/sqlparse/issues/543 for further information
                    assert isinstance(sub_token.left, Identifier)
                    assert isinstance(sub_token.right, Identifier)
                    self._target_tables.add(sub_token.left.get_real_name())
                    self._source_tables.add(sub_token.right.get_real_name())
                else:
                    assert isinstance(sub_token, Identifier)
                    self._target_tables.add(sub_token.get_real_name())
                target_table_token_flag = False
            elif temp_table_token_flag:
                if self.__token_negligible_before_tablename(sub_token):
                    continue
                else:
                    assert isinstance(sub_token, Identifier)
                    self._source_tables.add(sub_token.get_real_name())
                    self._target_tables.add(sub_token.get_real_name())
                    self._extract_from_DML(sub_token)
                    temp_table_token_flag = False

    def _extract_from_DDL_DROP(self, stmt: Statement) -> None:
        for st_tables in (self._source_tables, self._target_tables):
            st_tables -= {t.get_real_name() for t in stmt.tokens if isinstance(t, Identifier)}

    def _extract_from_DDL_ALTER(self, stmt: Statement) -> None:
        tables = [t.get_real_name() for t in stmt.tokens if isinstance(t, Identifier)]
        keywords = [t for t in stmt.tokens if t.ttype is Keyword]
        if any(k.normalized == "RENAME" for k in keywords) and len(tables) == 2:
            for st_tables in (self._source_tables, self._target_tables):
                if tables[0] in st_tables:
                    st_tables.remove(tables[0])
                    st_tables.add(tables[1])

    @classmethod
    def __token_negligible_before_tablename(cls, token: Token) -> bool:
        return token.is_whitespace or isinstance(token, Comment)


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
