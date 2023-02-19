from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import StatementLineageHolder, SubQueryLineageHolder
from sqllineage.core.models import AnalyzerContext
from sqllineage.core.parser.sqlfluff.models import SqlFluffTable
from sqllineage.core.parser.sqlfluff.subquery.lineage_holder_extractor import (
    LineageHolderExtractor,
)


class DdlDropExtractor(LineageHolderExtractor):
    """
    DDL Drop queries lineage extractor
    """

    DDL_DROP_STMT_TYPES = ["drop_table_statement"]

    def __init__(self, dialect: str):
        super().__init__(dialect)

    def can_extract(self, statement_type: str) -> bool:
        """
        Determine if the current lineage holder extractor can process the statement
        :param statement_type: a sqlfluff segment type
        """
        return statement_type in self.DDL_DROP_STMT_TYPES

    def extract(
        self,
        statement: BaseSegment,
        context: AnalyzerContext,
        is_sub_query: bool = False,
    ) -> SubQueryLineageHolder:
        """
        Extract lineage for a given statement.
        :param statement: a sqlfluff segment with a statement
        :param context: 'AnalyzerContext'
        :param is_sub_query: determine if the statement is bracketed or not
        :return 'SubQueryLineageHolder' object
        """
        holder = StatementLineageHolder()
        for table in {
            SqlFluffTable.of(t)
            for t in statement.segments
            if t.type == "table_reference"
        }:
            holder.add_drop(table)
        return holder