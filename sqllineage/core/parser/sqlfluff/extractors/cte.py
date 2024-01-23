from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.parser.sqlfluff.extractors.base import BaseExtractor
from sqllineage.core.parser.sqlfluff.extractors.create_insert import (
    CreateInsertExtractor,
)
from sqllineage.core.parser.sqlfluff.extractors.select import SelectExtractor
from sqllineage.core.parser.sqlfluff.extractors.update import UpdateExtractor
from sqllineage.core.parser.sqlfluff.models import SqlFluffSubQuery
from sqllineage.core.parser.sqlfluff.utils import list_child_segments
from sqllineage.utils.entities import AnalyzerContext


class CteExtractor(BaseExtractor):
    """
    CTE queries lineage extractor
    """

    SUPPORTED_STMT_TYPES = ["with_compound_statement"]

    def extract(
        self,
        statement: BaseSegment,
        context: AnalyzerContext,
    ) -> SubQueryLineageHolder:
        holder = self._init_holder(context)
        subqueries = []
        for segment in list_child_segments(statement):
            if segment.type in ["select_statement", "set_expression"]:
                holder |= self.delegate_to(
                    SelectExtractor,
                    segment,
                    AnalyzerContext(
                        cte=holder.cte,
                        write=holder.write,
                        write_columns=holder.write_columns,
                    ),
                )
            elif segment.type == "insert_statement":
                holder |= self.delegate_to(
                    CreateInsertExtractor, segment, AnalyzerContext(cte=holder.cte)
                )
            elif segment.type == "update_statement":
                holder |= self.delegate_to(
                    UpdateExtractor, segment, AnalyzerContext(cte=holder.cte)
                )
            elif segment.type == "common_table_expression":
                alias = None
                sub_segments = list_child_segments(segment)
                for sub_segment in sub_segments:
                    if sub_segment.type == "identifier":
                        alias = sub_segment.raw
                    elif sub_segment.type == "bracketed":
                        for sq in self.list_subquery(sub_segment):
                            sq.alias = alias
                            subqueries.append(sq)
                        holder.add_cte(SqlFluffSubQuery.of(sub_segment, alias))

        self.extract_subquery(subqueries, holder)

        return holder
