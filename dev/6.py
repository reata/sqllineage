import sys
from pathlib import Path
import json

import sqlfluff
from sqlfluff.core import Linter
from sqlfluff.core.parser import segments, BaseSegment
from sqlfluff.dialects.dialect_ansi import StatementSegment

import sqllineage.runner
from sqllineage.core.parser.sqlfluff.extractors.base import BaseExtractor
from sqllineage.core.parser.sqlfluff.utils import (
    list_child_segments,
    find_from_expression_element,
)


class dev:
    def __init__(self):
        self._sql = Path(sys.argv[0]).parent.joinpath("test.sql").read_text()

    def _list_table_from_from_clause_or_join_clause(self, segment: BaseSegment):
        """
        Extract table from from_clause or join_clause, join_clause is a child node of from_clause.
        """
        tables = []
        if segment.type in ["from_clause", "join_clause", "update_statement"]:
            if len(from_expressions := segment.get_children("from_expression")) > 1:
                # SQL89 style of join
                for from_expression in from_expressions:
                    if from_expression_element := find_from_expression_element(
                        from_expression
                    ):
                        tables += BaseExtractor._add_dataset_from_expression_element(
                            from_expression_element, holder
                        )
            else:
                if from_expression_element := find_from_expression_element(segment):
                    tables += self._add_dataset_from_expression_element(
                        from_expression_element, holder
                    )
                for join_clause in list_join_clause(segment):
                    tables += self._list_table_from_from_clause_or_join_clause(
                        join_clause, holder
                    )

    def print_seg(self, seg: segments):
        # x:segments
        print(seg)
        print("=======")
        for x in seg:
            x: segments
            self.print_seg(x.segments)

    def parse(self):
        statements = Linter(dialect="ansi").parse_string(self._sql).tree.segments[0]

        # for statement in statements.segments:
        #     if statement.type
        print(statements)

        # segments = (
        #     [statement]
        #     if statement.type == "set_expression"
        #     else list_child_segments(statement)
        # )
        # for x in segments:
        #     print(x )

    def do(self):
        self.parse()


def p0():
    sql = Path(sys.argv[0]).parent.joinpath("test.sql").read_text()
    # statements = Linter(dialect="postgres").parse_string(sql).tree.segments[0]
    # print(statements)
    a = sqlfluff.parse(sql=sql, dialect="hive")
    a = json.dumps(a)
    print(a)
    b = sqllineage.runner.LineageRunner(sql, "hive")
    b.print_table_lineage()

    b.print_column_lineage()


if __name__ == "__main__":
    p0()
    # dev = dev()
    # dev.do()
