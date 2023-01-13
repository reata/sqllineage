from sqlfluff.core.parser import BaseSegment

from sqllineage.sqlfluff_core.models import SqlFluffAnalyzerContext
from sqllineage.sqlfluff_core.holders import (
    SqlFluffSubQueryLineageHolder,
    SqlFluffStatementLineageHolder,
)
from sqllineage.sqlfluff_core.subquery.lineage_holder_extractor import (
    LineageHolderExtractor,
)
from sqllineage.sqlfluff_core.models import SqlFluffTable


class DdlDropExtractor(LineageHolderExtractor):

    DDL_DROP_STMT_TYPES = ["drop_table_statement"]

    def can_extract(self, statement_type: str):
        return statement_type in self.DDL_DROP_STMT_TYPES

    def extract(
        self,
        statement: BaseSegment,
        context: SqlFluffAnalyzerContext,
        is_sub_query: bool = False,
    ) -> SqlFluffSubQueryLineageHolder:
        holder = SqlFluffStatementLineageHolder()
        for table in {
            SqlFluffTable.of(t)
            for t in statement.segments
            if t.type == "table_reference"
        }:
            holder.add_drop(table)
        return holder
