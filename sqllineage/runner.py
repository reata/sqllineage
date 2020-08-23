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
        self, sql: str, encoding: str = None, verbose: bool = False,
    ):
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
        statements = self.statements(strip_comments=True)
        combined = """Statements(#): {stmt_cnt}
Source Tables:
    {source_tables}
Target Tables:
    {target_tables}
""".format(
            stmt_cnt=len(statements),
            source_tables="\n    ".join(str(t) for t in self.source_tables),
            target_tables="\n    ".join(str(t) for t in self.target_tables),
        )
        if self.intermediate_tables:
            combined += """Intermediate Tables:
    {intermediate_tables}""".format(
                intermediate_tables="\n    ".join(
                    str(t) for t in self.intermediate_tables
                )
            )
        if self._verbose:
            result = ""
            for i, lineage_result in enumerate(self._lineage_results):
                stmt_short = statements[i].replace("\n", "")
                if len(stmt_short) > 50:
                    stmt_short = stmt_short[:50] + "..."
                result += """Statement #{ord}: {stmt}
    {content}
""".format(
                    ord=i + 1,
                    content=str(lineage_result).replace("\n", "\n    "),
                    stmt=stmt_short,
                )
            combined = result + "==========\nSummary:\n" + combined
        return combined

    def draw(self) -> None:
        return draw_lineage_graph(self._combined_lineage_result.lineage_graph)

    @property
    def statements_parsed(self) -> List[Statement]:
        return self._stmt

    def statements(self, **kwargs) -> List[str]:
        return [sqlparse.format(s.value, **kwargs) for s in self.statements_parsed]

    @property
    def source_tables(self) -> List[Table]:
        return sorted(self._combined_lineage_result.source_tables, key=lambda x: str(x))

    @property
    def target_tables(self) -> List[Table]:
        return sorted(self._combined_lineage_result.target_tables, key=lambda x: str(x))

    @property
    def intermediate_tables(self) -> List[Table]:
        return sorted(
            self._combined_lineage_result.intermediate_tables, key=lambda x: str(x)
        )


def main(args=None) -> None:
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
