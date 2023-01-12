from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import StatementLineageHolder
from sqllineage.sqlfluff_core.models import SqlFluffAnalyzerContext
from sqllineage.sqlfluff_core.subquery.cte_extractor import DmlCteExtractor
from sqllineage.sqlfluff_core.subquery.ddl_alter_extractor import DdlAlterExtractor
from sqllineage.sqlfluff_core.subquery.ddl_drop_extractor import DdlDropExtractor
from sqllineage.sqlfluff_core.subquery.dml_insert_extractor import DmlInsertExtractor
from sqllineage.sqlfluff_core.subquery.dml_select_extractor import DmlSelectExtractor
from sqllineage.sqlfluff_core.subquery.lineage_holder_extractor import (
    LineageHolderExtractor,
)
from sqllineage.sqlfluff_core.subquery.noop_extractor import NoopExtractor

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
        """Analyze the base segment and store the result into `sqllineage.holders.StatementLineageHolder` class.

        Args:
            statement (BaseSegment): a SQL base segment parsed by `sqlfluff`
            dialect (str): dialect used to parse the statement
            is_sub_query (bool): the original query contained parentheses

        Returns:
            `sqllineage.holders.StatementLineageHolder` object
        """
        subquery_extractors = [
            extractor_cls(dialect)
            for extractor_cls in LineageHolderExtractor.__subclasses__()
        ]
        for subquery_extractor in subquery_extractors:
            if subquery_extractor.can_extract(statement.type):
                lineage_holder = subquery_extractor.extract(
                    statement, SqlFluffAnalyzerContext(), is_sub_query
                )
                if lineage_holder:
                    return StatementLineageHolder.of(lineage_holder)
                return StatementLineageHolder()
        raise NotImplementedError(
            f"Can not extract lineage for dialect [{dialect}] from query: [{statement.raw}]"
        )

    @staticmethod
    def can_analyze(stmt: BaseSegment):
        """
        Check if the current lineage analyzer can analyze the statement

        :param stmt: a SQL base segment parsed by `sqlfluff`
        """
        return stmt.type in SUPPORTED_STMT_TYPES
