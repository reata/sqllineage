import warnings

from sqlfluff.core import (
    FluffConfig,
    Linter,
    SQLLexError,
    SQLParseError,
    dialect_readout,
)
from sqlfluff.core.parser import BaseSegment

from sqllineage.core.analyzer import LineageAnalyzer
from sqllineage.core.holders import StatementLineageHolder
from sqllineage.core.metadata_provider import MetaDataProvider
from sqllineage.core.parser.sqlfluff.extractors.base import BaseExtractor
from sqllineage.exceptions import (
    InvalidSyntaxException,
    UnsupportedStatementException,
)
from sqllineage.utils.entities import AnalyzerContext


class SqlFluffLineageAnalyzer(LineageAnalyzer):
    """SQL Statement Level Lineage Analyzer for `sqlfluff`"""

    PARSER_NAME = "sqlfluff"
    SUPPORTED_DIALECTS = list(dialect.label for dialect in dialect_readout())

    def __init__(
        self,
        sql: str,
        file_path: str,
        dialect: str,
        silent_mode: bool = False,
    ):
        super().__init__(sql)
        self._sqlfluff_config = FluffConfig.from_path(
            path=file_path, overrides={"dialect": dialect}
        )
        self._dialect = dialect
        self._silent_mode = silent_mode
        self._segment_cache: dict[str, BaseSegment] = {}
        self._stmts = self._split_and_cache()

    def _split_and_cache(self) -> list[str]:
        """Parse SQL once, split into statements, and cache segment per statement."""
        stripped = self._sql.strip()
        if not stripped:
            return []
        # sqlfluff handles multi-statement SQL natively, including TSQL
        # without semicolons — no need for separate TSQL_NO_SEMICOLON path.
        segments = self._list_specific_statement_segment(stripped)
        stmts = []
        for segment in segments:
            self._segment_cache[segment.raw] = segment
            # Append trailing semicolon to match sqlparse split behavior, which
            # preserves the semicolon as part of the statement string.
            stmts.append(segment.raw + ";")
        return stmts

    @property
    def statements(self) -> list[str]:
        return self._stmts

    def analyze(
        self, sql: str, metadata_provider: MetaDataProvider
    ) -> StatementLineageHolder:
        if (cache_key := sql.rstrip(";")) in self._segment_cache:
            statement_segments = [self._segment_cache[cache_key]]
        else:
            statement_segments = self._list_specific_statement_segment(sql)
        if len(statement_segments) == 0:
            raise UnsupportedStatementException(
                f"SQLLineage cannot parse SQL:{sql}"
            )  # pragma: no cover
        else:
            statement_segment = statement_segments[0]
            holder = BaseExtractor.try_extract(
                self._sqlfluff_config.get("dialect"),
                metadata_provider,
                statement_segment,
                AnalyzerContext(),
            )
            if holder is not None:
                return StatementLineageHolder.of(holder)
            else:
                if self._silent_mode:
                    warnings.warn(
                        f"SQLLineage doesn't support analyzing statement type [{statement_segment.type}] for SQL:{sql}"
                    )
                    return StatementLineageHolder()
                else:
                    raise UnsupportedStatementException(
                        f"SQLLineage doesn't support analyzing statement type [{statement_segment.type}] for SQL:{sql}"
                    )

    def _list_specific_statement_segment(self, sql: str):
        parsed = Linter(config=self._sqlfluff_config).parse_string(sql)
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
        segments = []
        for top_segment in getattr(parsed.tree, "segments", []):
            match top_segment.type:
                case "statement":
                    segments.append(top_segment.segments[0])
                case "batch":
                    statements = top_segment.get_children("statement")
                    if len(statements) > 1:
                        warnings.warn(
                            "SQL statements is not split by semicolon. "
                            "SQLLineage is not guaranteed to generate correct result under this circumstances.",
                            SyntaxWarning,
                            stacklevel=2,
                        )
                    for statement in statements:
                        segments.append(statement.segments[0])
        return segments
