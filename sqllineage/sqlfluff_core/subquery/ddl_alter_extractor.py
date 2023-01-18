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


class DdlAlterExtractor(LineageHolderExtractor):
    """
    DDL Alter queries lineage extractor
    """

    DDL_ALTER_STMT_TYPES = [
        "alter_table_statement",
        "rename_statement",
        "rename_table_statement",
    ]

    def __init__(self, dialect: str):
        super().__init__(dialect)

    def can_extract(self, statement_type: str) -> bool:
        """
        Determine if the current lineage holder extractor can process the statement
        :param statement_type: a sqlfluff segment type
        """
        return statement_type in self.DDL_ALTER_STMT_TYPES

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
        tables = []
        for t in statement.segments:
            if t.type == "table_reference":
                tables.append(SqlFluffTable.of(t))
        keywords = [t for t in statement.segments if t.type == "keyword"]
        if any(k.raw_upper == "RENAME" for k in keywords):
            if statement.type == "alter_table_statement" and len(tables) == 2:
                holder.add_rename(tables[0], tables[1])
        if any(k.raw_upper == "EXCHANGE" for k in keywords) and len(tables) == 2:
            holder.add_write(tables[0])
            holder.add_read(tables[1])
        return holder
