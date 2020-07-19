import argparse
import importlib
import sys
from typing import List, Set, Type

import sqlparse
from sqlparse.sql import Statement

from sqllineage.combiners import DefaultLineageCombiner, LineageCombiner
from sqllineage.core import LineageAnalyzer
from sqllineage.models import Table


class LineageRunner(object):
    def __init__(
        self,
        sql: str,
        encoding: str = None,
        combiner: Type[LineageCombiner] = DefaultLineageCombiner,
        verbose: bool = False,
    ):
        self._encoding = encoding
        self._stmt = [
            s
            for s in sqlparse.parse(sql.strip(), self._encoding)
            if s.token_first(skip_cm=True)
        ]
        self._lineage_results = [LineageAnalyzer().analyze(stmt) for stmt in self._stmt]
        self._lineage_result = combiner.combine(*self._lineage_results)
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

    @property
    def statements_parsed(self) -> List[Statement]:
        return self._stmt

    def statements(self, **kwargs) -> List[str]:
        return [sqlparse.format(s.value, **kwargs) for s in self.statements_parsed]

    @property
    def source_tables(self) -> Set[Table]:
        return self._lineage_result.read

    @property
    def target_tables(self) -> Set[Table]:
        return self._lineage_result.write

    @property
    def intermediate_tables(self) -> Set[Table]:
        return self._lineage_result.intermediate


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="sqllineage", description="SQL Lineage Parser."
    )
    parser.add_argument(
        "-e", metavar="<quoted-query-string>", help="SQL from command line"
    )
    parser.add_argument("-f", metavar="<filename>", help="SQL from files")
    parser.add_argument(
        "-c",
        metavar="<combiner>",
        help="specify Combiner to combine lineage result from multiple statements, "
        "Default Value: sqllineage.combiners.DefaultLineageCombiner",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        help="increase output verbosity, show statement level lineage result",
        action="store_true",
    )
    args = parser.parse_args()
    combiner = DefaultLineageCombiner
    if args.c:
        try:
            combiner_module, combiner_name = args.c.rsplit(".", maxsplit=1)
            combiner = getattr(importlib.import_module(combiner_module), combiner_name)
        except ValueError:
            print(
                "ERROR: Combiner should be named as module.class, got {} instead".format(
                    args.c
                ),
                file=sys.stderr,
            )
            raise
        except (ImportError, AttributeError):
            print("ERROR: No such combiner: {}".format(args.c), file=sys.stderr)
            raise
    if args.e and args.f:
        print(
            "WARNING: Both -e and -f options are specified. -e option will be ignored",
            file=sys.stderr,
        )
    if args.f:
        try:
            with open(args.f) as f:
                sql = f.read()
            print(LineageRunner(sql, combiner=combiner, verbose=args.verbose))
        except FileNotFoundError:
            print("ERROR: No such file: {}".format(args.f), file=sys.stderr)
        except PermissionError:
            print(
                "ERROR: Permission denied when reading file '{}'".format(args.f),
                file=sys.stderr,
            )
    elif args.e:
        print(LineageRunner(args.e, combiner=combiner, verbose=args.verbose))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
