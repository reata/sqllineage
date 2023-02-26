from sqlfluff.core import Linter
from sqlfluff.core.parser import BaseSegment

from sqllineage.core.analyzer import LineageAnalyzer
from sqllineage.core.exceptions import SQLLineageException
from sqllineage.core.holders import (
    StatementLineageHolder,
    SubQueryLineageHolder,
)
from sqllineage.core.models import AnalyzerContext
from sqllineage.core.parser.sqlfluff.subquery.cte_extractor import DmlCteExtractor
from sqllineage.core.parser.sqlfluff.subquery.ddl_alter_extractor import (
    DdlAlterExtractor,
)
from sqllineage.core.parser.sqlfluff.subquery.ddl_drop_extractor import DdlDropExtractor
from sqllineage.core.parser.sqlfluff.subquery.dml_insert_extractor import (
    DmlInsertExtractor,
)
from sqllineage.core.parser.sqlfluff.subquery.dml_select_extractor import (
    DmlSelectExtractor,
)
from sqllineage.core.parser.sqlfluff.subquery.noop_extractor import NoopExtractor
from sqllineage.core.parser.sqlfluff.utils.sqlfluff import (
    clean_parentheses,
    get_statement_segment,
    is_subquery_statement,
    remove_statement_parentheses,
)

SUPPORTED_STMT_TYPES = (
    DmlSelectExtractor.DML_SELECT_STMT_TYPES
    + DmlInsertExtractor.DML_INSERT_STMT_TYPES
    + DmlCteExtractor.CTE_STMT_TYPES
    + DdlDropExtractor.DDL_DROP_STMT_TYPES
    + DdlAlterExtractor.DDL_ALTER_STMT_TYPES
    + NoopExtractor.NOOP_STMT_TYPES
)


class SqlFluffLineageAnalyzer(LineageAnalyzer):
    """SQL Statement Level Lineage Analyzer for `sqlfluff`"""

    def __init__(self, dialect: str):
        self._dialect = dialect

    def analyze(self, sql: str) -> StatementLineageHolder:
        # remove nested parentheses that sqlfluff cannot parse
        sql = clean_parentheses(sql)
        is_sub_query = is_subquery_statement(sql)
        if is_sub_query:
            sql = remove_statement_parentheses(sql)
        linter = Linter(dialect=self._dialect)
        parsed_string = linter.parse_string(sql)
        statement_segment = get_statement_segment(parsed_string)
        if statement_segment and SqlFluffLineageAnalyzer.can_analyze(statement_segment):
            if "unparsable" in statement_segment.descendant_type_set:
                raise SQLLineageException(
                    f"The query [\n{sql}\n] contains an unparsable segment."
                )
        else:
            raise SQLLineageException(
                f"The query [\n{sql}\n] contains can not be analyzed."
            )

        subquery_extractors = [
            DmlSelectExtractor(self._dialect),
            DmlInsertExtractor(self._dialect),
            DmlCteExtractor(self._dialect),
            DdlDropExtractor(self._dialect),
            DdlAlterExtractor(self._dialect),
            NoopExtractor(self._dialect),
        ]
        lineage_holder = SubQueryLineageHolder()
        for subquery_extractor in subquery_extractors:
            if subquery_extractor.can_extract(statement_segment.type):
                lineage_holder = subquery_extractor.extract(
                    statement_segment, AnalyzerContext(), is_sub_query
                )
        return StatementLineageHolder.of(lineage_holder)

    @staticmethod
    def can_analyze(statement: BaseSegment):
        """
        Check if the current lineage analyzer can analyze the statement
        :param statement: a SQL base segment parsed by `sqlfluff`
        """
        return statement.type in SUPPORTED_STMT_TYPES
