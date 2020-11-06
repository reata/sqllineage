import re
from typing import Set, TYPE_CHECKING, Tuple

from sqlparse.sql import (
    Comment,
    Comparison,
    Function,
    Identifier,
    IdentifierList,
    Parenthesis,
    Statement,
    TokenList,
)
from sqlparse.tokens import Keyword, Token

from sqllineage.exceptions import SQLLineageException
from sqllineage.models import Table

SOURCE_TABLE_TOKENS = (
    r"FROM",
    # inspired by https://github.com/andialbrecht/sqlparse/blob/master/sqlparse/keywords.py
    r"((LEFT\s+|RIGHT\s+|FULL\s+)?(INNER\s+|OUTER\s+|STRAIGHT\s+)?|(CROSS\s+|NATURAL\s+)?)?JOIN",
)
TARGET_TABLE_TOKENS = ("INTO", "OVERWRITE", "TABLE", "VIEW")
TEMP_TABLE_TOKENS = ("WITH",)


class LineageResult:
    """
    Statement Level Lineage Result.

    LineageResult will hold attributes like read, write, rename, drop, intermediate.

    Each of them is a Set[:class:`sqllineage.models.Table`] except for rename.

    For rename, it a Set[Tuple[:class:`sqllineage.models.Table`, :class:`sqllineage.models.Table`]], with the first
    table being original table before renaming and the latter after renaming.

    This is the most atomic representation of lineage result.
    """

    __slots__ = ["read", "write", "rename", "drop", "intermediate"]
    if TYPE_CHECKING:
        read = write = drop = intermediate = set()  # type: Set[Table]
        rename = set()  # type: Set[Tuple[Table, Table]]

    def __init__(self) -> None:
        for attr in self.__slots__:
            setattr(self, attr, set())

    def __add__(self, other):
        lineage_result = LineageResult()
        for attr in self.__slots__:
            setattr(
                lineage_result, attr, getattr(self, attr).union(getattr(other, attr))
            )
        return lineage_result

    def __str__(self):
        return "\n".join(
            f"table {attr}: {sorted(getattr(self, attr), key=lambda x: str(x)) if getattr(self, attr) else '[]'}"
            for attr in self.__slots__
        )

    def __repr__(self):
        return str(self)


class LineageAnalyzer:
    """SQL Statement Level Lineage Analyzer."""

    def __init__(self) -> None:
        self._lineage_result = LineageResult()

    def analyze(self, stmt: Statement) -> LineageResult:
        """
        to analyze the Statement and store the result into :class:`LineageResult`.

        :param stmt: a SQL statement parsed by `sqlparse`
        """
        if stmt.get_type() == "DROP":
            self._extract_from_DDL_DROP(stmt)
        elif stmt.get_type() == "ALTER":
            self._extract_from_DDL_ALTER(stmt)
        elif (
            stmt.get_type() == "DELETE"
            or stmt.token_first(skip_cm=True).normalized == "TRUNCATE"
            or stmt.token_first(skip_cm=True).normalized.upper() == "REFRESH"
            or stmt.token_first(skip_cm=True).normalized == "CACHE"
        ):
            pass
        else:
            # DML parsing logic also applies to CREATE DDL
            self._extract_from_DML(stmt)
        return self._lineage_result

    def _extract_from_DDL_DROP(self, stmt: Statement) -> None:
        for table in {
            Table.create(t) for t in stmt.tokens if isinstance(t, Identifier)
        }:
            self._lineage_result.drop.add(table)

    def _extract_from_DDL_ALTER(self, stmt: Statement) -> None:
        tables = [Table.create(t) for t in stmt.tokens if isinstance(t, Identifier)]
        keywords = [t for t in stmt.tokens if t.ttype is Keyword]
        if any(k.normalized == "RENAME" for k in keywords) and len(tables) == 2:
            self._lineage_result.rename.add((tables[0], tables[1]))

    def _extract_from_DML(self, token: Token) -> None:
        source_table_token_flag = (
            target_table_token_flag
        ) = temp_table_token_flag = False
        for sub_token in token.tokens:
            if isinstance(sub_token, TokenList):
                self._extract_from_DML(sub_token)
            if sub_token.ttype in Keyword:
                if any(
                    re.match(regex, sub_token.normalized)
                    for regex in SOURCE_TABLE_TOKENS
                ):
                    source_table_token_flag = True
                elif sub_token.normalized in TARGET_TABLE_TOKENS:
                    target_table_token_flag = True
                elif sub_token.normalized in TEMP_TABLE_TOKENS:
                    temp_table_token_flag = True
                continue
            if source_table_token_flag:
                if self.__token_negligible_before_tablename(sub_token):
                    continue
                else:
                    if isinstance(sub_token, Identifier):
                        if isinstance(sub_token.token_first(skip_cm=True), Parenthesis):
                            # SELECT col1 FROM (SELECT col2 FROM tab1) dt, the subquery will be parsed as Identifier
                            # and this Identifier's get_real_name method would return alias name dt
                            # referring https://github.com/andialbrecht/sqlparse/issues/218 for further information
                            pass
                        else:
                            self._lineage_result.read.add(Table.create(sub_token))
                    elif isinstance(sub_token, IdentifierList):
                        # This is to support join in ANSI-89 syntax
                        for token in sub_token.tokens:
                            if isinstance(token, Identifier):
                                self._lineage_result.read.add(Table.create(token))
                    elif isinstance(sub_token, Parenthesis):
                        # SELECT col1 FROM (SELECT col2 FROM tab1), the subquery will be parsed as Parenthesis
                        # This syntax without alias for subquery is invalid in MySQL, while valid for SparkSQL
                        pass
                    else:
                        raise SQLLineageException(
                            "An Identifier is expected, got %s[value: %s] instead"
                            % (type(sub_token).__name__, sub_token)
                        )
                    source_table_token_flag = False
            elif target_table_token_flag:
                if self.__token_negligible_before_tablename(sub_token):
                    continue
                elif isinstance(sub_token, Function):
                    # insert into tab (col1, col2) values (val1, val2); Here tab (col1, col2) will be parsed as Function
                    # referring https://github.com/andialbrecht/sqlparse/issues/483 for further information
                    if not isinstance(sub_token.token_first(skip_cm=True), Identifier):
                        raise SQLLineageException(
                            "An Identifier is expected, got %s[value: %s] instead"
                            % (type(sub_token).__name__, sub_token)
                        )
                    self._lineage_result.write.add(
                        Table.create(sub_token.token_first(skip_cm=True))
                    )
                elif isinstance(sub_token, Comparison):
                    # create table tab1 like tab2, tab1 like tab2 will be parsed as Comparison
                    # referring https://github.com/andialbrecht/sqlparse/issues/543 for further information
                    if not (
                        isinstance(sub_token.left, Identifier)
                        and isinstance(sub_token.right, Identifier)
                    ):
                        raise SQLLineageException(
                            "An Identifier is expected, got %s[value: %s] instead"
                            % (type(sub_token).__name__, sub_token)
                        )
                    self._lineage_result.write.add(Table.create(sub_token.left))
                    self._lineage_result.read.add(Table.create(sub_token.right))
                else:
                    if not isinstance(sub_token, Identifier):
                        raise SQLLineageException(
                            "An Identifier is expected, got %s[value: %s] instead"
                            % (type(sub_token).__name__, sub_token)
                        )
                    self._lineage_result.write.add(Table.create(sub_token))
                target_table_token_flag = False
            elif temp_table_token_flag:
                if self.__token_negligible_before_tablename(sub_token):
                    continue
                else:
                    if not isinstance(sub_token, Identifier):
                        raise SQLLineageException(
                            "An Identifier is expected, got %s[value: %s] instead"
                            % (type(sub_token).__name__, sub_token)
                        )
                    self._lineage_result.intermediate.add(Table.create(sub_token))
                    self._extract_from_DML(sub_token)
                    temp_table_token_flag = False

    @classmethod
    def __token_negligible_before_tablename(cls, token: Token) -> bool:
        return token.is_whitespace or isinstance(token, Comment)
