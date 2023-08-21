import warnings

from sqlfluff.core import Linter, SQLLexError, SQLParseError

from sqllineage.core.analyzer import LineageAnalyzer
from sqllineage.core.holders import StatementLineageHolder
from sqllineage.core.parser.sqlfluff.extractors.base import BaseExtractor
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
        parsed = Linter(dialect=self._dialect).parse_string(sql)
        violations = [
            str(e)
            for e in parsed.violations
            if isinstance(e, (SQLLexError, SQLParseError))
        ]
        if violations:
            violation_msg = "\n".join(violations)
            raise InvalidSyntaxException(
                f"This SQL statement is unparsable, please check potential syntax error for SQL:\n"
                f"{sql}\n"
                f"{violation_msg}"
            )

        statement_segment = None
        for top_segment in getattr(parsed.tree, "segments", []):
            if top_segment.type == "statement":
                statement_segment = top_segment.segments[0]
                break
            elif top_segment.type == "batch":
                statements = top_segment.get_children("statement")
                if len(statements) > 1:
                    warnings.warn(
                        "SQL statements is not split by semicolon. "
                        "SQLLineage is not guaranteed to generate correct result under this circumstances.",
                        SyntaxWarning,
                        stacklevel=2,
                    )
                statement_segment = statements[0].segments[0]
                break
        if statement_segment is None:
            raise UnsupportedStatementException(
                f"SQLLineage cannot parse SQL:" f"{sql}"
            )  # pragma: no cover
        else:
            for extractor in [
                extractor_cls(self._dialect)
                for extractor_cls in BaseExtractor.__subclasses__()
            ]:
                if extractor.can_extract(statement_segment.type):
                    lineage_holder = extractor.extract(
                        statement_segment, AnalyzerContext()
                    )
                    return StatementLineageHolder.of(lineage_holder)
            else:
                raise UnsupportedStatementException(
                    f"SQLLineage doesn't support analyzing statement type [{statement_segment.type}] for SQL:"
                    f"{sql}"
                )
