import logging
from typing import Dict, List, Optional, Tuple

import sqlparse
from sqlfluff.api.simple import get_simple_config
from sqlfluff.core import Linter
from sqlparse.sql import Statement

from sqllineage.core import LineageAnalyzer
from sqllineage.core.exceptions import SQLLineageException
from sqllineage.core.holders import SQLLineageHolder, StatementLineageHolder
from sqllineage.core.sqlfluff.analyzer import SqlFluffLineageAnalyzer
from sqllineage.core.sqlfluff.utils.sqlfluff import get_statement_segment
from sqllineage.core.sqlparse.models import SqlParseColumn, SqlParseTable
from sqllineage.drawing import draw_lineage_graph
from sqllineage.io import to_cytoscape
from sqllineage.utils.constant import LineageLevel
from sqllineage.utils.helpers import (
    clean_parentheses,
    is_subquery_statement,
    remove_statement_parentheses,
)

logger = logging.getLogger(__name__)


def lazy_method(func):
    def wrapper(*args, **kwargs):
        self = args[0]
        if not self._evaluated:
            self._eval()
        return func(*args, **kwargs)

    return wrapper


def lazy_property(func):
    return property(lazy_method(func))


class LineageRunner(object):
    def __init__(
        self,
        sql: str,
        encoding: Optional[str] = None,
        verbose: bool = False,
        draw_options: Optional[Dict[str, str]] = None,
        dialect: Optional[str] = "ansi",
        use_sqlfluff: bool = False,
    ):
        """
        The entry point of SQLLineage after command line options are parsed.

        :param sql: a string representation of SQL statements.
        :param encoding: the encoding for sql string
        :param verbose: verbose flag indicate whether statement-wise lineage result will be shown
        """
        self._encoding = encoding
        self._sql = sql
        self._verbose = verbose
        self._draw_options = draw_options if draw_options else {}
        self._evaluated = False
        self._stmt: List[Statement] = []
        self._use_sqlfluff = use_sqlfluff
        if self._use_sqlfluff:
            self._sqlfluff_linter = Linter(
                config=get_simple_config(dialect=dialect, config_path=None)
            )
            self._dialect = dialect

    @lazy_method
    def __str__(self):
        """
        print out the Lineage Summary.
        """
        statements = self.statements(strip_comments=True)
        source_tables = "\n    ".join(str(t) for t in self.source_tables)
        target_tables = "\n    ".join(str(t) for t in self.target_tables)
        combined = f"""Statements(#): {len(statements)}
Source Tables:
    {source_tables}
Target Tables:
    {target_tables}
"""
        if self.intermediate_tables:
            intermediate_tables = "\n    ".join(
                str(t) for t in self.intermediate_tables
            )
            combined += f"""Intermediate Tables:
    {intermediate_tables}"""
        if self._verbose:
            result = ""
            for i, holder in enumerate(self._stmt_holders):
                stmt_short = statements[i].replace("\n", "")
                if len(stmt_short) > 50:
                    stmt_short = stmt_short[:50] + "..."
                content = str(holder).replace("\n", "\n    ")
                result += f"""Statement #{i + 1}: {stmt_short}
    {content}
"""
            combined = result + "==========\nSummary:\n" + combined
        return combined

    @lazy_method
    def to_cytoscape(self, level=LineageLevel.TABLE) -> List[Dict[str, Dict[str, str]]]:
        """
        to turn the DAG into cytoscape format.
        """
        if level == LineageLevel.COLUMN:
            return to_cytoscape(self._sql_holder.column_lineage_graph, compound=True)
        else:
            return to_cytoscape(self._sql_holder.table_lineage_graph)

    def draw(self, dialect: str, use_sqlfluff: bool) -> None:
        """
        to draw the lineage directed graph
        """
        draw_options = self._draw_options
        if draw_options.get("f") is None:
            draw_options.pop("f", None)
            draw_options["e"] = self._sql
            draw_options["dialect"] = dialect
            draw_options["use_sqlfluff"] = str(use_sqlfluff)
        return draw_lineage_graph(**draw_options)

    @lazy_method
    def statements(self, **kwargs) -> List[str]:
        """
        a list of statements.

        :param kwargs: the key arguments that will be passed to `sqlparse.format`
        """
        return [sqlparse.format(s.value, **kwargs) for s in self.statements_parsed]

    @lazy_property
    def statements_parsed(self) -> List[Statement]:
        """
        a list of :class:`sqlparse.sql.Statement`
        """
        return self._stmt

    @lazy_property
    def source_tables(self) -> List[SqlParseTable]:
        """
        a list of source :class:`sqllineage.models.Table`
        """
        return sorted(self._sql_holder.source_tables, key=lambda x: str(x))

    @lazy_property
    def target_tables(self) -> List[SqlParseTable]:
        """
        a list of target :class:`sqllineage.models.Table`
        """
        return sorted(self._sql_holder.target_tables, key=lambda x: str(x))

    @lazy_property
    def intermediate_tables(self) -> List[SqlParseTable]:
        """
        a list of intermediate :class:`sqllineage.models.Table`
        """
        return sorted(self._sql_holder.intermediate_tables, key=lambda x: str(x))

    @lazy_method
    def get_column_lineage(
        self, exclude_subquery=True
    ) -> List[Tuple[SqlParseColumn, SqlParseColumn]]:
        """
        a list of column tuple :class:`sqllineage.models.Column`
        """
        # sort by target column, and then source column
        return sorted(
            self._sql_holder.get_column_lineage(exclude_subquery),
            key=lambda x: (str(x[-1]), str(x[0])),
        )

    def print_column_lineage(self) -> None:
        """
        print column level lineage to stdout
        """
        for path in self.get_column_lineage():
            print(" <- ".join(str(col) for col in reversed(path)))

    def print_table_lineage(self) -> None:
        """
        print table level lineage to stdout
        """
        print(str(self))

    def _eval(self):
        self._stmt = [
            s
            for s in sqlparse.parse(
                # first apply sqlparser formatting just to get rid of comments, which cause
                # inconsistencies in parsing output
                clean_parentheses(
                    sqlparse.format(
                        self._sql.strip(), self._encoding, strip_comments=True
                    )
                ),
                self._encoding,
            )
            if s.token_first(skip_cm=True)
        ]

        self._stmt_holders = [self.run_lineage_analyzer(stmt) for stmt in self._stmt]
        self._sql_holder = SQLLineageHolder.of(*self._stmt_holders)
        self._evaluated = True

    def run_lineage_analyzer(self, stmt: Statement) -> StatementLineageHolder:
        stmt_value = stmt.value.strip()
        if self._use_sqlfluff:
            is_sub_query = is_subquery_statement(stmt_value)
            if is_sub_query:
                stmt_value = remove_statement_parentheses(stmt_value)
            parsed_string = self._sqlfluff_linter.parse_string(stmt_value)
            statement_segment = get_statement_segment(parsed_string)
            if statement_segment and SqlFluffLineageAnalyzer.can_analyze(
                statement_segment
            ):
                if "unparsable" in statement_segment.descendant_type_set:
                    raise SQLLineageException(
                        f"The query [\n{stmt_value}\n] contains an unparsable segment."
                    )
                return SqlFluffLineageAnalyzer().analyze(
                    statement_segment, self._dialect or "", is_sub_query
                )
            else:
                raise SQLLineageException(
                    f"The query [\n{stmt_value}\n] contains can not be analyzed."
                )
        return LineageAnalyzer().analyze(stmt)
