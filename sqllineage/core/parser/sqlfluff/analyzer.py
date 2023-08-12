from sqlfluff.core import Linter

from sqllineage.core.analyzer import LineageAnalyzer
from sqllineage.core.holders import (
    StatementLineageHolder,
    SubQueryLineageHolder,
)
from sqllineage.core.parser.sqlfluff.extractors.lineage_holder_extractor import (
    LineageHolderExtractor,
)
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
        parsed = linter.parse_string(sql)
        statement_segment = None
        for top_segment in getattr(parsed.tree, "segments", []):
            if top_segment.type == "statement":
                statement_segment = top_segment.segments[0]
                break
            elif top_segment.type == "batch":
                statement_segment = top_segment.get_child("statement").segments[0]
                break
        if statement_segment is None:
            raise UnsupportedStatementException(
                f"SQLLineage cannot parse SQL:" f"{sql}"
            )  # pragma: no cover
        else:
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
