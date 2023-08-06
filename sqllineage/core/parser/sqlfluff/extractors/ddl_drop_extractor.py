from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import StatementLineageHolder, SubQueryLineageHolder
from sqllineage.core.parser.sqlfluff.extractors.lineage_holder_extractor import (
    LineageHolderExtractor,
)
from sqllineage.core.parser.sqlfluff.models import SqlFluffTable
from sqllineage.utils.entities import AnalyzerContext


class DdlDropExtractor(LineageHolderExtractor):
    """
    DDL Drop queries lineage extractor
    """

    SUPPORTED_STMT_TYPES = ["drop_table_statement"]

    def __init__(self, dialect: str):
        super().__init__(dialect)

    def extract(
        self,
        statement: BaseSegment,
        context: AnalyzerContext,
    ) -> SubQueryLineageHolder:
        """
        Extract lineage for a given statement.
        :param statement: a sqlfluff segment with a statement
        :param context: 'AnalyzerContext'
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
