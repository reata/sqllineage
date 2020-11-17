import itertools
import re
from typing import Set, TYPE_CHECKING, Tuple, List, Dict

import networkx as nx
from sqlparse.sql import (
    Case,
    Comment,
    Comparison,
    Function,
    Identifier,
    IdentifierList,
    Operation,
    Parenthesis,
    Statement,
    TokenList,
)
from sqlparse.tokens import Keyword, Token

# from sqllineage.combiners import combine_columns
from sqllineage.exceptions import SQLLineageException
from sqllineage.models import Table, Column


SOURCE_TABLE_TOKENS = (
    r"FROM",
    # inspired by https://github.com/andialbrecht/sqlparse/blob/master/sqlparse/keywords.py
    r"((LEFT\s+|RIGHT\s+|FULL\s+)?(INNER\s+|OUTER\s+|STRAIGHT\s+)?|(CROSS\s+|NATURAL\s+)?)?JOIN",
)
TARGET_TABLE_TOKENS = ("INTO", "OVERWRITE", "TABLE", "VIEW")
# TEMP_TABLE_TOKENS = ("WITH(?!.+?SCHEMA BIND.+?)",)
TEMP_TABLE_TOKENS = ("WITH",)
TARGET_COLUMN_TOKENS = ("SELECT",)
# Partial list of unhandled tokens:
#   AND, AS, CREATE, DESC, EXISTS, FULL, IF, INSERT, NOT, ON, OR, PARTITION,
#   SHOW, UNION, UNION ALL, VALUES, WHERE,


class DataSetLineage:
    """
    Statement Level Lineage Result.

    LineageResult will hold attributes like read, write, rename, drop, intermediate.

    Each of them is a Set[:class:`sqllineage.models.Table`] except for rename.

    For rename, it a Set[Tuple[:class:`sqllineage.models.Table`, :class:`sqllineage.models.Table`]], with the first
    table being original table before renaming and the latter after renaming.

    This is the most atomic representation of lineage result.
    """

    __slots__ = ["read", "write", "rename", "drop", "intermediate", "columns"]
    if TYPE_CHECKING:
        read = write = drop = intermediate = set()  # type: Set[Table]
        rename = set()  # type: Set[Tuple[Table, Table]]

    def __init__(self) -> None:
        for attr in self.__slots__:
            setattr(self, attr, set())

    def __add__(self, other):
        lineage_result = DataSetLineage()
        for attr in self.__slots__:
            setattr(lineage_result, attr, getattr(self, attr).union(getattr(other, attr)))
        return lineage_result

    def __str__(self):
        return "\n".join(
            f"table {attr}: {sorted(getattr(self, attr), key=lambda x: str(x)) if getattr(self, attr) else '[]'}"
            for attr in self.__slots__
        )

    def __repr__(self):
        return str(self)

    def __bool__(self):
        return any(getattr(self, s) for s in self.__slots__)


class LineageAnalyzer:
    """SQL Statement Level Lineage Analyzer."""

    def __init__(self) -> None:
        self._lineage_result = DataSetLineage()

        self._clause_tokens = {
            "select": TARGET_COLUMN_TOKENS,
            "source": SOURCE_TABLE_TOKENS,
            "target": TARGET_TABLE_TOKENS,
            "temp": TEMP_TABLE_TOKENS,
        }
        self._clause_methods = {
            "select": self._handle_target_column_token,
            "source": self._handle_source_table_token,
            "target": self._handle_target_table_token,
            "temp": self._handle_temp_table_token,
        }

        # _columns is kept as a separate list because the incomplete Identifiers are added
        # there until they can be "fully qualified" and put into _columns_graph.
        self._columns = []
        self._columns_graph = nx.DiGraph()

    def analyze(self, stmt: Statement) -> Tuple[DataSetLineage, nx.DiGraph]:
        """
        to analyze the Statement and store the result into :class:`LineageResult`.

        :param stmt: a SQL statement parsed by `sqlparse`
        """
        if stmt.get_type() == "DROP":
            self._extract_from_ddl_drop(stmt)
        elif stmt.get_type() == "ALTER":
            self._extract_from_ddl_alter(stmt)
        elif (
            stmt.get_type() == "DELETE"
            or stmt.token_first(skip_cm=True).normalized == "TRUNCATE"
            or stmt.token_first(skip_cm=True).normalized.upper() == "REFRESH"
            or stmt.token_first(skip_cm=True).normalized == "CACHE"
        ):
            pass
        else:
            # DML parsing logic also applies to CREATE DDL
            self._extract_from_dml(stmt)
            self._columns_build_network()

        return self._lineage_result, self._columns_graph

    def _extract_from_ddl_drop(self, stmt: Statement) -> None:
        for table in {Table.create(t) for t in stmt.tokens if isinstance(t, Identifier)}:
            self._lineage_result.drop.add(table)

    def _extract_from_ddl_alter(self, stmt: Statement) -> None:
        tables = [Table.create(t) for t in stmt.tokens if isinstance(t, Identifier)]
        keywords = [t for t in stmt.tokens if t.ttype is Keyword]
        if any(k.normalized == "RENAME" for k in keywords) and len(tables) == 2:
            self._lineage_result.rename.add((tables[0], tables[1]))

    def _extract_from_dml(self, token: Token) -> None:
        table_cmd_stack = []
        unhandled_stack = []

        for tn, sub_token in enumerate(token.tokens):
            if self.__token_negligible_before_tablename(sub_token):
                continue

            if isinstance(sub_token, TokenList):
                self._extract_from_dml(sub_token)
                # There is no continue here because things like table names are inside a TokenList,
                # so _extract_from_dml() will do nothing, after which the logic below will execute.

            if sub_token.ttype in Keyword:
                for k, v in self._clause_tokens.items():
                    if any(re.match(regex, sub_token.normalized) for regex in v):
                        # Sometimes multiple matching operators exist for a single clause--don't add extras.
                        if not table_cmd_stack or table_cmd_stack[-1] != k:
                            table_cmd_stack.append(k)
                        break
                else:
                    unhandled_stack.append(sub_token.normalized)
                continue

            if not len(table_cmd_stack):
                # print(type(sub_token), sub_token, "    whole: ", token)
                continue

            # noinspection PyArgumentList
            self._clause_methods[table_cmd_stack.pop()](sub_token)

        # if unhandled_stack:
        #     print("Unhandled: ", unhandled_stack)

        if table_cmd_stack:
            raise SQLLineageException(f"Clause keywords without content: {table_cmd_stack}")

    def _handle_source_table_token(self, sub_token: Token) -> None:
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
            self._raise_identifier_exception(sub_token)

    def _handle_target_table_token(self, sub_token: Token) -> None:
        if isinstance(sub_token, Function):
            # insert into tab (col1, col2) values (val1, val2); Here tab (col1, col2) will be parsed as Function
            # referring https://github.com/andialbrecht/sqlparse/issues/483 for further information
            if not isinstance(sub_token.token_first(skip_cm=True), Identifier):
                self._raise_identifier_exception(sub_token)
            self._lineage_result.write.add(Table.create(sub_token.token_first(skip_cm=True)))
        elif isinstance(sub_token, Comparison):
            # create table tab1 like tab2, tab1 like tab2 will be parsed as Comparison
            # referring https://github.com/andialbrecht/sqlparse/issues/543 for further information
            if not (
                isinstance(sub_token.left, Identifier) and isinstance(sub_token.right, Identifier)
            ):
                self._raise_identifier_exception(sub_token)
            self._lineage_result.write.add(Table.create(sub_token.left))
            self._lineage_result.read.add(Table.create(sub_token.right))
        else:
            if not isinstance(sub_token, Identifier):
                self._raise_identifier_exception(sub_token)
            self._lineage_result.write.add(Table.create(sub_token))

    def _handle_temp_table_token(self, sub_token: Token) -> None:
        if not isinstance(sub_token, Identifier):
            self._raise_identifier_exception(sub_token)
        if sub_token.normalized == "BINDING":  # Ignore view clause "WITH NO SCHEMA BINDING"
            return
        self._lineage_result.intermediate.add(Table.create(sub_token))
        self._extract_from_dml(sub_token)

    def _handle_target_column_token(self, sub_token: Token) -> None:
        not_identifier = (",", "1", "*")

        if not isinstance(sub_token, IdentifierList):
            sub_token = [sub_token]

        for st in sub_token:
            # Functions aren't handled. Yet.
            if isinstance(st, Function):
                continue
            # No information here
            if st.is_whitespace or st.normalized in not_identifier:
                continue
            if isinstance(st, IdentifierList):
                print("Complex expressions aren't handled")
                continue
            if st.ttype in Token.Keyword:
                print("Complex expressions aren't handled")
                continue
            if st.ttype in Token.Literal:
                # This shouldn't happen but sometimes it does anyway due to weird SQL
                continue
            if not isinstance(st, Identifier):
                self._raise_identifier_exception(sub_token)
            self._columns.append(st)

    def _columns_build_network(self) -> None:
        """Build out a DiGraph from the columns information

        This is used rather than a DataSetLineage as with table names because
        the relations are more complex and would require a separate Lineage
        object to be created for every column.  Rather than doing that,
        a graph object handling the relationships is built right away.
        """
        # Get the source table aliases
        table_mapping = self._build_table_mapping()
        for c in self._columns:
            sources, target = self._columns_get_sources_target(c, table_mapping)
            self._columns_graph.add_nodes_from(sources, source=True)
            self._columns_graph.add_node(target, target=True)
            for src, tgt in itertools.product(sources, [target]):
                self._columns_graph.add_edge(src, tgt)

    @staticmethod
    def _columns_get_sources_target(column: Identifier, tables: Dict) -> Tuple[List, Column]:
        parent_table = (
            tables[column.get_parent_name()]
            if column.get_parent_name() in tables
            else Table("unknown")
        )
        if not column.has_alias():
            # If there is no alias then the source and target columns names are the same.
            # Create a mapping from the column parent name (source table) to the target
            # if column.get_parent_name() not in tables:
            #     raise KeyError(f"Unknown table {column.get_parent_name()} for column {str(column)}")
            return [Column(column.get_name(), parent_table)], Column(
                column.get_name(), tables[None]
            )
        else:
            # This is a simple column with an alias, e.g. "schema.table.field1 as field"
            if column[0].ttype is Token.Name:
                return [Column(column.get_real_name(), parent_table)], Column(
                    column.get_alias(), tables[None]
                )
            # Source is a function
            elif isinstance(column[0], Function):
                # print("Functions are unhandled")
                cols = LineageAnalyzer._extract_source_columns(column[0], skip=1)
                cols = [Column(c.get_name(), tables[c.get_parent_name()]) for c in cols]
                # try:
                #     cols = [Column(c.get_name(), tables[c.get_parent_name()]) for c in cols]
                # except KeyError as e:
                #     print(f"Missing table {e} so skipping source {column[0]}")
                #     return [], Column(column.get_alias(), tables[None])
                return cols, Column(column.get_alias(), tables[None])
                # return [Column("FUNCTION", Table("unknown"))], Column(column.get_alias(), tables[None])
            # Source is an operation
            elif isinstance(column[0], Operation):
                print("Operations are unhandled")
                return [Column("OPERATION", Table("unknown"))], Column(
                    column.get_alias(), tables[None]
                )
            # Source is a case statement
            elif isinstance(column[0], Case):
                print("Case statements are unhandled")
                return [Column("CASE", Table("unknown"))], Column(column.get_alias(), tables[None])
            # Source is a literal
            elif column[0].ttype in Token.Literal:
                return [Column("LITERAL", Table("unknown"))], Column(
                    column.get_alias(), tables[None]
                )
            # Shouldn't get here
            else:
                raise SQLLineageException(f"Unhandled column string: {column}")

    @staticmethod
    def _extract_source_columns(token: Token, skip=0) -> List:
        # print(str(token))
        columns = []
        for t in token[skip:]:
            if isinstance(t, Identifier):
                columns.append(t)
                continue
            elif t.ttype is Token.Punctuation:
                continue
            elif t.is_group:
                columns.extend(
                    LineageAnalyzer._extract_source_columns(
                        t, skip=1 if isinstance(t, Function) else 0
                    )
                )
        return columns

    def _build_table_mapping(self) -> Dict:
        def _mapping_helper(table: Table):
            # Note that if there are conflicting 'raw_name's where tables have the
            # same name in different schemas, then this can be broken depending
            # on how the DML uses aliases.  But it should work in most cases.
            m = {
                table.raw_name: table,
                str(table): table,
            }
            if table.alias:
                m[table.alias] = table
            return m

        mapping = {}
        # Targets first (this is on purpose because the first target table will be used if no specifier is given)
        for tgt in self._lineage_result.write:
            mapping.update(_mapping_helper(tgt))
        for tmp in self._lineage_result.intermediate:
            mapping.update(_mapping_helper(tmp))
        for src in self._lineage_result.read:
            mapping.update(_mapping_helper(src))
        # Make the None key map to whatever table was added first
        if mapping:
            mapping[None] = mapping[next(iter(mapping))]
        return mapping

    @staticmethod
    def _raise_identifier_exception(sub_token):
        raise SQLLineageException(
            "An Identifier is expected, got %s[value: %s] instead"
            % (type(sub_token).__name__, sub_token)
        )

    @classmethod
    def __token_negligible_before_tablename(cls, token: Token) -> bool:
        return token.is_whitespace or isinstance(token, Comment)
