import argparse
import logging
from typing import List

import sqlparse
from sqlparse.sql import Statement

from sqllineage.combiners import combine
from sqllineage.core import LineageAnalyzer
from sqllineage.drawing import draw_lineage_graph
from sqllineage.models import Table

logger = logging.getLogger(__name__)


class LineageRunner(object):
    def __init__(
        self,
        sql: str,
        encoding: str = None,
        verbose: bool = False,
    ):
        """
        The entry point of SQLLineage after command line options are parsed.

        :param sql: a string representation of SQL statements.
        :param encoding: the encoding for sql string
        :param verbose: verbose flag indicate whether statement-wise lineage result will be shown
        """
        self._encoding = encoding
        self._stmt = [
            s
            for s in sqlparse.parse(sql.strip(), self._encoding)
            if s.token_first(skip_cm=True)
        ]
        self._lineage_results = [LineageAnalyzer().analyze(stmt) for stmt in self._stmt]
        self._combined_lineage_result = combine(*self._lineage_results)
        self._verbose = verbose

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
            for i, lineage_result in enumerate(self._lineage_results):
                stmt_short = statements[i].replace("\n", "")
                if len(stmt_short) > 50:
                    stmt_short = stmt_short[:50] + "..."
                content = str(lineage_result).replace("\n", "\n    ")
                result += f"""Statement #{i + 1}: {stmt_short}
    {content}
"""
            combined = result + "==========\nSummary:\n" + combined
        return combined

    def draw(self) -> None:
        """
        to draw the lineage directed graph with matplotlib.
        """
        return draw_lineage_graph(self._combined_lineage_result.lineage_graph)

    def statements(self, **kwargs) -> List[str]:
        """
        a list of statements.

        :param kwargs: the key arguments that will be passed to `sqlparse.format`
        """
        return [sqlparse.format(s.value, **kwargs) for s in self.statements_parsed]

    @property
    def statements_parsed(self) -> List[Statement]:
        """
        a list of :class:`sqlparse.sql.Statement`
        """
        return self._stmt

    @property
    def source_tables(self) -> List[Table]:
        """
        a list of source :class:`sqllineage.models.Table`
        """
        return sorted(self._combined_lineage_result.source_tables, key=lambda x: str(x))

    @property
    def target_tables(self) -> List[Table]:
        """
        a list of target :class:`sqllineage.models.Table`
        """
        return sorted(self._combined_lineage_result.target_tables, key=lambda x: str(x))

    @property
    def intermediate_tables(self) -> List[Table]:
        """
        a list of intermediate :class:`sqllineage.models.Table`
        """
        return sorted(
            self._combined_lineage_result.intermediate_tables, key=lambda x: str(x)
        )


def main(args=None) -> None:
    """
    The command line interface entry point.

    :param args: the command line arguments for sqllineage command
    """
    parser = argparse.ArgumentParser(
        prog="sqllineage", description="SQL Lineage Parser."
    )
    parser.add_argument(
        "-e", metavar="<quoted-query-string>", help="SQL from command line"
    )
    parser.add_argument("-f", metavar="<filename>", help="SQL from files")
    parser.add_argument(
        "-v",
        "--verbose",
        help="increase output verbosity, show statement level lineage result",
        action="store_true",
    )
    parser.add_argument(
        "-g",
        "--graphviz",
        help="show graph visualization of the lineage with graphviz dot layout",
        action="store_true",
    )
    args = parser.parse_args(args)
    if args.e and args.f:
        logging.warning(
            "Both -e and -f options are specified. -e option will be ignored"
        )
    if args.f or args.e:
        sql = ""
        if args.f:
            try:
                with open(args.f) as f:
                    sql = f.read()
            except IsADirectoryError:
                logger.exception("%s is a directory", args.f)
                exit(1)
            except FileNotFoundError:
                logger.exception("No such file: %s", args.f)
                exit(1)
            except PermissionError:
                logger.exception("Permission denied when reading file '%s'", args.f)
                exit(1)
        elif args.e:
            sql = args.e
        runner = LineageRunner(sql, verbose=args.verbose)
        if args.graphviz:
            runner.draw()
        else:
            print(runner)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
