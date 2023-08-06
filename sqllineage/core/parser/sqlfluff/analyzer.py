from sqlfluff.core import Linter

from sqllineage.core.analyzer import LineageAnalyzer
from sqllineage.core.holders import (
    StatementLineageHolder,
    SubQueryLineageHolder,
)
from sqllineage.core.parser.sqlfluff.extractors.lineage_holder_extractor import (
    LineageHolderExtractor,
)
from sqllineage.core.parser.sqlfluff.utils import get_statement_segment
from sqllineage.exceptions import (
    InvalidSyntaxException,
    UnsupportedStatementException,
)
from sqllineage.utils.entities import AnalyzerContext


class SqlFluffLineageAnalyzer(LineageAnalyzer):
    """SQL Statement Level Lineage Analyzer for `sqlfluff`"""

    def __init__(self, dialect: str):
        self._dialect = dialect

    def analyze(self, sql: str) -> StatementLineageHolder:
        linter = Linter(dialect=self._dialect)
        parsed_string = linter.parse_string(sql)
        statement_segment = get_statement_segment(parsed_string)
        extractors = [
            extractor_cls(self._dialect)
            for extractor_cls in LineageHolderExtractor.__subclasses__()
        ]
        if statement_segment.type == "unparsable":
            raise InvalidSyntaxException(
                f"This SQL statement is unparsable, please check potential syntax error for SQL:"
                f"{sql}"
            )
        else:
            if any(
                extractor.can_extract(statement_segment.type)
                for extractor in extractors
            ):
                if "unparsable" in statement_segment.descendant_type_set:
                    raise InvalidSyntaxException(
                        f"{statement_segment.type} is partially unparsable, "
                        f"please check potential syntax error for SQL:"
                        f"{sql}"
                    )
                else:
                    lineage_holder = SubQueryLineageHolder()
                    for extractor in extractors:
                        if extractor.can_extract(statement_segment.type):
                            lineage_holder = extractor.extract(
                                statement_segment, AnalyzerContext()
                            )
                            break
                    return StatementLineageHolder.of(lineage_holder)
            else:
                raise UnsupportedStatementException(
                    f"SQLLineage doesn't support analyzing statement type [{statement_segment.type}] for SQL:"
                    f"{sql}"
                )
