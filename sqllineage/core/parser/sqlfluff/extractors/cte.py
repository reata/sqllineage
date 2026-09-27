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
        ctes = []
        for segment in list_child_segments(statement):
            match segment.type:
                case "select_statement" | "set_expression":
                    holder |= self.delegate_to(
                        SelectExtractor,
                        segment,
                        AnalyzerContext(
                            cte=holder.cte,
                            write=holder.write,
                            write_columns=holder.write_columns,
                        ),
                    )
                case "insert_statement":
                    holder |= self.delegate_to(
                        CreateInsertExtractor, segment, AnalyzerContext(cte=holder.cte)
                    )
                case "update_statement":
                    holder |= self.delegate_to(
                        UpdateExtractor, segment, AnalyzerContext(cte=holder.cte)
                    )
                case "common_table_expression":
                    alias = None
                    for sub_segment in list_child_segments(segment):
                        match sub_segment.type:
                            case "identifier":
                                alias = sub_segment.raw
                            case "bracketed":
                                for sq in self.list_subquery(sub_segment):
                                    sq.alias = alias
                                    subqueries.append(sq)
                                cte = SqlFluffSubQuery.of(sub_segment, alias)
                                holder.add_cte(cte)
                                ctes.append(cte)

        self.extract_subquery(subqueries, holder)

        # A wildcard selected from a CTE can only be expanded once the CTE's
        # columns are known, i.e. after its query is extracted above. Expand in
        # definition order so that a CTE can select * from an earlier one.
        for cte in ctes:
            holder.expand_wildcard(self.metadata_provider, cte)
        holder.expand_wildcard(self.metadata_provider)

        return holder
