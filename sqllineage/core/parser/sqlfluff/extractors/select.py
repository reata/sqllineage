from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.models import Path
from sqllineage.core.parser import SourceHandlerMixin
from sqllineage.core.parser.sqlfluff.extractors.base import BaseExtractor
from sqllineage.core.parser.sqlfluff.models import (
    SqlFluffColumn,
    SqlFluffSubQuery,
    SqlFluffTable,
)
from sqllineage.core.parser.sqlfluff.utils import (
    find_from_expression_element,
    find_table_identifier,
    is_set_expression,
    list_child_segments,
    list_join_clause,
    list_subqueries,
)
from sqllineage.utils.entities import AnalyzerContext
from sqllineage.utils.helpers import escape_identifier_name


class SelectExtractor(BaseExtractor, SourceHandlerMixin):
    """
    Select statement lineage extractor
    """

    SUPPORTED_STMT_TYPES = ["select_statement", "set_expression", "bracketed"]

    def __init__(self, dialect: str):
        super().__init__(dialect)
        self.columns = []
        self.tables = []
        self.union_barriers = []

    def extract(
        self,
        statement: BaseSegment,
        context: AnalyzerContext,
    ) -> SubQueryLineageHolder:
        holder = self._init_holder(context)
        subqueries = []
        segments = (
            [statement]
            if statement.type == "set_expression"
            else list_child_segments(statement)
        )
        for segment in segments:
            for sq in self.list_subquery(segment):
                # Collecting subquery on the way, hold on parsing until last
                # so that each handler don't have to worry about what's inside subquery
                subqueries.append(sq)

            self._handle_swap_partition(segment, holder)
            self._handle_select_into(segment, holder)
            self._handle_table(segment, holder)
            self._handle_column(segment)
            self._handle_set(segment)

        self.end_of_query_cleanup(holder)

        # By recursively extracting each subquery of the parent and merge, we're doing Depth-first search
        for sq in subqueries:
            holder |= SelectExtractor(self.dialect).extract(
                sq.query, AnalyzerContext(cte=holder.cte, write={sq})
            )

        return holder

    @staticmethod
    def _handle_swap_partition(segment: BaseSegment, holder: SubQueryLineageHolder):
        """
        A handler for swap_partitions_between_tables function
        """
        if segment.type == "select_clause":
            if select_clause_element := segment.get_child("select_clause_element"):
                if function := select_clause_element.get_child("function"):
                    if (
                        function.first_non_whitespace_segment_raw_upper
                        == "SWAP_PARTITIONS_BETWEEN_TABLES"
                    ):
                        if bracketed := function.get_child("bracketed"):
                            expressions = bracketed.get_children("expression")
                            holder.add_read(
                                SqlFluffTable(
                                    escape_identifier_name(expressions[0].raw)
                                )
                            )
                            holder.add_write(
                                SqlFluffTable(
                                    escape_identifier_name(expressions[3].raw)
                                )
                            )

    def _handle_select_into(self, segment: BaseSegment, holder: SubQueryLineageHolder):
        """
        A handler for SELECT INTO statement supported by some dialects
        """
        if segment.type in ["into_table_clause", "into_clause"]:
            if identifier := find_table_identifier(segment):
                if table := self.find_table(identifier):
                    holder.add_write(table)

    def _handle_table(
        self, segment: BaseSegment, holder: SubQueryLineageHolder
    ) -> None:
        """
        handle from_clause or join_clause, join_clause is a child node of from_clause.
        """
        if segment.type in ["from_clause", "join_clause"]:
            from_expressions = segment.get_children("from_expression")
            if len(from_expressions) > 1:
                # SQL89 style of join
                for from_expression in from_expressions:
                    if from_expression_element := find_from_expression_element(
                        from_expression
                    ):
                        self._add_dataset_from_expression_element(
                            from_expression_element, holder
                        )
            else:
                if from_expression_element := find_from_expression_element(segment):
                    self._add_dataset_from_expression_element(
                        from_expression_element, holder
                    )
                for join_clause in list_join_clause(segment):
                    self._handle_table(join_clause, holder)

    def _handle_column(self, segment: BaseSegment) -> None:
        """
        Column handler method
        """
        if segment.type == "select_clause":
            for sub_segment in segment.get_children("select_clause_element"):
                self.columns.append(SqlFluffColumn.of(sub_segment))

    def _handle_set(self, segment: BaseSegment) -> None:
        """
        set handler method
        """
        if is_set_expression(segment):
            subqueries = list_subqueries(segment)
            if subqueries:
                for idx, sq in enumerate(subqueries):
                    if idx != 0:
                        self.union_barriers.append(
                            (len(self.columns), len(self.tables))
                        )
                    subquery, alias = sq
                    table_identifier = find_table_identifier(subquery)
                    if table_identifier:
                        read_sq = SqlFluffTable.of(table_identifier, alias)
                        for seg in list_child_segments(subquery):
                            self._handle_column(seg)
                        self.tables.append(read_sq)

    def _add_dataset_from_expression_element(
        self, segment: BaseSegment, holder: SubQueryLineageHolder
    ) -> None:
        """
        Append tables and subqueries identified in the 'from_expression_element' type segment to the table and
        holder extra subqueries sets
        """
        all_segments = [
            seg for seg in list_child_segments(segment) if seg.type != "keyword"
        ]
        if table_expression := segment.get_child("table_expression"):
            if table_expression.get_child("function"):
                # for UNNEST or generator function, no dataset involved
                return
        first_segment = all_segments[0]
        if first_segment.type == "bracketed":
            if table_expression := first_segment.get_child("table_expression"):
                if table_expression.get_child("values_clause"):
                    # (VALUES ...) AS alias, no dataset involved
                    return
        subqueries = list_subqueries(segment)
        if subqueries:
            for sq in subqueries:
                bracketed, alias = sq
                read_sq = SqlFluffSubQuery.of(bracketed, alias)
                self.tables.append(read_sq)
        else:
            table_identifier = find_table_identifier(segment)
            if table_identifier:
                subquery_flag = False
                alias = None
                if len(all_segments) > 1 and all_segments[1].type == "alias_expression":
                    all_segments = list_child_segments(all_segments[1])
                    alias = str(
                        all_segments[1].raw
                        if len(all_segments) > 1
                        else all_segments[0].raw
                    )
                if "." not in table_identifier.raw:
                    cte_dict = {s.alias: s for s in holder.cte}
                    cte = cte_dict.get(table_identifier.raw)
                    if cte is not None:
                        # could reference CTE with or without alias
                        self.tables.append(
                            SqlFluffSubQuery.of(
                                cte.query,
                                alias or table_identifier.raw,
                            )
                        )
                        subquery_flag = True
                if subquery_flag is False:
                    if table_identifier.type == "file_reference":
                        self.tables.append(
                            Path(
                                escape_identifier_name(
                                    table_identifier.segments[-1].raw
                                )
                            )
                        )
                    else:
                        self.tables.append(
                            SqlFluffTable.of(table_identifier, alias=alias)
                        )
