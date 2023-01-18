from sqlfluff.core.parser import BaseSegment

from sqllineage.sqlfluff_core.holders import (
    SqlFluffStatementLineageHolder,
    SqlFluffSubQueryLineageHolder,
)
from sqllineage.sqlfluff_core.models import SqlFluffAnalyzerContext
from sqllineage.sqlfluff_core.models import SqlFluffTable
from sqllineage.sqlfluff_core.subquery.lineage_holder_extractor import (
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
        context: SqlFluffAnalyzerContext,
        is_sub_query: bool = False,
    ) -> SqlFluffSubQueryLineageHolder:
        """
        Extract lineage for a given statement.
        :param statement: a sqlfluff segment with a statement
        :param context: 'SqlFluffAnalyzerContext'
        :param is_sub_query: determine if the statement is bracketed or not
        :return 'SqlFluffSubQueryLineageHolder' object
        """
        holder = SqlFluffStatementLineageHolder()
        for table in {
            SqlFluffTable.of(t)
            for t in statement.segments
            if t.type == "table_reference"
        }:
            holder.add_drop(table)
        return holder
