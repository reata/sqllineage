from sqlfluff.core.parser import BaseSegment

from sqllineage.core.analyzer import AnalyzerContext
from sqllineage.core.holders import SubQueryLineageHolder, StatementLineageHolder
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
        context: AnalyzerContext,
        is_sub_query: bool = False,
    ) -> SubQueryLineageHolder:
        holder = StatementLineageHolder()
        for table in {
            SqlFluffTable.of(t)
            for t in statement.segments
            if t.type == "table_reference"
        }:
            holder.add_drop(table)
        return holder
