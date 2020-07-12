import argparse
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
    ):
        self._encoding = encoding
        self._stmt = [
            s
            for s in sqlparse.parse(sql.strip(), self._encoding)
            if s.token_first(skip_cm=True)
        ]
        lineage_results = [LineageAnalyzer().analyze(stmt) for stmt in self._stmt]
        self._lineage_result = combiner.combine(*lineage_results)

    def __str__(self):
        return """Statements(#): {stmt_cnt}
Source Tables:
    {source_tables}
Target Tables:
    {target_tables}
""".format(
            stmt_cnt=len(self.statements),
            source_tables="\n    ".join(str(t) for t in self.source_tables),
            target_tables="\n    ".join(str(t) for t in self.target_tables),
        )

    @property
    def statements_parsed(self) -> List[Statement]:
        return self._stmt

    @property
    def statements(self) -> List[str]:
        return [sqlparse.format(s.value) for s in self.statements_parsed]

    @property
    def source_tables(self) -> Set[Table]:
        return {t for t in self._lineage_result.read}

    @property
    def target_tables(self) -> Set[Table]:
        return {t for t in self._lineage_result.write}


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="sqllineage", description="SQL Lineage Parser."
    )
    parser.add_argument(
        "-e", metavar="<quoted-query-string>", help="SQL from command line"
    )
    parser.add_argument("-f", metavar="<filename>", help="SQL from files")
    args = parser.parse_args()
    if args.e and args.f:
        print(
            "WARNING: Both -e and -f options are specified. -e option will be ignored",
            file=sys.stderr,
        )
    if args.f:
        try:
            with open(args.f) as f:
                sql = f.read()
            print(LineageRunner(sql))
        except FileNotFoundError:
            print("ERROR: No such file: {}".format(args.f), file=sys.stderr)
        except PermissionError:
            print(
                "ERROR: Permission denied when reading file '{}'".format(args.f),
                file=sys.stderr,
            )
    elif args.e:
        print(LineageRunner(args.e))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
