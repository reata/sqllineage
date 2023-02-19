from sqlfluff.core.parser import BaseSegment

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

SUPPORTED_STMT_TYPES = (
    DmlSelectExtractor.DML_SELECT_STMT_TYPES
    + DmlInsertExtractor.DML_INSERT_STMT_TYPES
    + DmlCteExtractor.CTE_STMT_TYPES
    + DdlDropExtractor.DDL_DROP_STMT_TYPES
    + DdlAlterExtractor.DDL_ALTER_STMT_TYPES
    + NoopExtractor.NOOP_STMT_TYPES
)


class SqlFluffLineageAnalyzer:
    """SQL Statement Level Lineage Analyzer for `sqlfluff`"""

    def analyze(
        self, statement: BaseSegment, dialect: str, is_sub_query: bool = False
    ) -> StatementLineageHolder:
        """
        Analyze the base segment and store the result into `sqllineage.holders.StatementLineageHolder` class.
        :param statement: a SQL base segment parsed by `sqlfluff`
        :param dialect: dialect used to parse the statement
        :param is_sub_query: the original query contained parentheses
        :return: 'SqlFluffStatementLineageHolder' object
        """
        subquery_extractors = [
            DmlSelectExtractor(dialect),
            DmlInsertExtractor(dialect),
            DmlCteExtractor(dialect),
            DdlDropExtractor(dialect),
            DdlAlterExtractor(dialect),
            NoopExtractor(dialect),
        ]
        lineage_holder = SubQueryLineageHolder()
        for subquery_extractor in subquery_extractors:
            if subquery_extractor.can_extract(statement.type):
                lineage_holder = subquery_extractor.extract(
                    statement, AnalyzerContext(), is_sub_query
                )
        return StatementLineageHolder.of(lineage_holder)

    @staticmethod
    def can_analyze(statement: BaseSegment):
        """
        Check if the current lineage analyzer can analyze the statement
        :param statement: a SQL base segment parsed by `sqlfluff`
        """
        return statement.type in SUPPORTED_STMT_TYPES