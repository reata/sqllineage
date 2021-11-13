import logging
from typing import Dict, List, Tuple

import sqlparse
from sqlparse.sql import Statement

from sqllineage.core import LineageAnalyzer
from sqllineage.core.holders import SQLLineageHolder
from sqllineage.core.models import Column, Table
from sqllineage.drawing import draw_lineage_graph
from sqllineage.io import to_cytoscape
from sqllineage.utils.constant import LineageLevel


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
        encoding: str = None,
        verbose: bool = False,
        draw_options: Dict[str, str] = None,
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

    def draw(self) -> None:
        """
        to draw the lineage directed graph
        """
        draw_options = self._draw_options
        if draw_options.get("f") is None:
            draw_options.pop("f", None)
            draw_options["e"] = self._sql
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
    def source_tables(self) -> List[Table]:
        """
        a list of source :class:`sqllineage.models.Table`
        """
        return sorted(self._sql_holder.source_tables, key=lambda x: str(x))

    @lazy_property
    def target_tables(self) -> List[Table]:
        """
        a list of target :class:`sqllineage.models.Table`
        """
        return sorted(self._sql_holder.target_tables, key=lambda x: str(x))

    @lazy_property
    def intermediate_tables(self) -> List[Table]:
        """
        a list of intermediate :class:`sqllineage.models.Table`
        """
        return sorted(self._sql_holder.intermediate_tables, key=lambda x: str(x))

    @lazy_method
    def get_column_lineage(self, exclude_subquery=True) -> List[Tuple[Column, Column]]:
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
                sqlparse.format(self._sql.strip(), self._encoding, strip_comments=True),
                self._encoding,
            )
            if s.token_first(skip_cm=True)
        ]
        self._stmt_holders = [LineageAnalyzer().analyze(stmt) for stmt in self._stmt]
        self._sql_holder = SQLLineageHolder.of(*self._stmt_holders)
        self._evaluated = True
