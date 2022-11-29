from functools import reduce
from operator import add
from typing import List, Tuple

from sqlfluff.core.parser import BaseSegment

from sqllineage.core.analyzer import AnalyzerContext
from sqllineage.core.holders import StatementLineageHolder, SubQueryLineageHolder
from sqllineage.sqlfluff_core.handlers.base import (
    SegmentBaseHandler,
    ConditionalSegmentBaseHandler,
)
from sqllineage.sqlfluff_core.models import SqlFluffTable, SqlFluffSubQuery
from sqllineage.sqlfluff_core.utils.entities import SubSqlFluffQueryTuple
from sqllineage.sqlfluff_core.utils.sqlfluff import (
    is_segment_negligible,
    is_subquery,
    get_subqueries,
    get_multiple_identifiers,
)


class SqlFluffLineageAnalyzer:
    """SQL Statement Level Lineage Analyzer for `sqlfluff`"""

    # question: should be the analyzer dialect specific?
    def analyze(self, stmt: BaseSegment) -> StatementLineageHolder:
        """Analyze the base segment and store the result into `sqllineage.holders.StatementLineageHolder` class.

        Args:
            stmt (BaseSegment): a SQL base segment parsed by `sqlfluff`

        Returns:
            `sqllineage.holders.StatementLineageHolder` object
        """
        # this could change from one dialect to another
        if (
            stmt.type == "delete_statement"
            or stmt.type == "truncate_table"
            or stmt.type == "refresh_statement"
            or stmt.type == "cache_table"
            or stmt.type == "uncache_table"
            or stmt.type == "show_statement"
        ):
            holder = StatementLineageHolder()
        elif stmt.type == "drop_table_statement":
            holder = self._extract_from_ddl_drop(stmt)
        elif (
            stmt.type == "alter_table_statement"
            or stmt.type == "rename_statement"
            or stmt.type == "rename_table_statement"
        ):
            holder = self._extract_from_ddl_alter(stmt)
        elif stmt.type == "select_statement":
            holder = StatementLineageHolder.of(
                self._extract_from_dml_select(stmt, AnalyzerContext())
            )
        elif stmt.type == "insert_statement":
            holder = StatementLineageHolder.of(
                self._extract_from_dml_insert(stmt, AnalyzerContext())
            )

        else:
            raise NotImplementedError("Can not extract from DML queries")
        return holder

    @classmethod
    def _extract_from_ddl_drop(cls, stmt: BaseSegment) -> StatementLineageHolder:
        holder = StatementLineageHolder()
        for table in {
            SqlFluffTable.of(t) for t in stmt.segments if t.type == "table_reference"
        }:
            holder.add_drop(table)
        return holder

    @classmethod
    def _extract_from_ddl_alter(cls, stmt: BaseSegment) -> StatementLineageHolder:
        holder = StatementLineageHolder()
        tables = []
        for t in stmt.segments:
            if t.type == "table_reference":
                tables.append(SqlFluffTable.of(t))
            # question: not sure why more than 2 tables will be in an alter statement
            # elif isinstance(t, IdentifierList):
            #     for identifier in t.get_identifiers():
            #         tables.append(Table.of(identifier))
        keywords = [t for t in stmt.segments if t.type == "keyword"]
        if any(k.raw_upper == "RENAME" for k in keywords):
            if stmt.get_type() == "alter_table_statement" and len(tables) == 2:
                holder.add_rename(tables[0], tables[1])
            # question: not sure why more than 2 tables will be in an alter statement when renaming
            # elif (
            #     stmt.token_first(skip_cm=True).normalized == "RENAME"
            #     and len(tables) % 2 == 0
            # ):
            #     for i in range(0, len(tables), 2):
            #         holder.add_rename(tables[i], tables[i + 1])
        # question: should be the analyzer dialect specific?
        if any(k.raw_upper == "EXCHANGE" for k in keywords) and len(tables) == 2:
            holder.add_write(tables[0])
            holder.add_read(tables[1])
        return holder

    @classmethod
    def _extract_from_dml_select(
        cls, statement: BaseSegment, context: AnalyzerContext
    ) -> SubQueryLineageHolder:

        handlers, conditional_handlers = cls._init_handlers()

        holder = cls._init_holder(context)

        subqueries = []
        segments = cls._retrieve_segments(statement)
        for segment in segments:
            for sq in cls.parse_subquery(segment):
                # Collecting subquery on the way, hold on parsing until last
                # so that each handler don't have to worry about what's inside subquery
                subqueries.append(sq)

            for current_handler in handlers:
                current_handler.handle(segment, holder)

            for conditional_handler in conditional_handlers:
                if conditional_handler.indicate(segment):
                    conditional_handler.handle(segment, holder)
        else:
            # call end of query hook here as loop is over
            for conditional_handler in conditional_handlers:
                conditional_handler.end_of_query_cleanup(holder)
        # By recursively extracting each subquery of the parent and merge, we're doing Depth-first search
        for sq in subqueries:
            holder |= cls._extract_from_dml_select(
                sq.segment, AnalyzerContext(sq, holder.cte)
            )
        return holder
    
    @classmethod
    def _extract_from_dml_insert(
        cls, statement: BaseSegment, context: AnalyzerContext
    ) -> SubQueryLineageHolder:

        handlers, conditional_handlers = cls._init_handlers()

        holder = cls._init_holder(context)

        subqueries = []
        select_statements = [] 
        segments = cls._retrieve_segments(statement)
        for segment in segments:
            for sq in cls.parse_subquery(segment):
                # Collecting subquery on the way, hold on parsing until last
                # so that each handler don't have to worry about what's inside subquery
                subqueries.append(sq)

            for current_handler in handlers:
                current_handler.handle(segment, holder)
            
            if segment.type == "select_statement":
                select_statements.append(segment)
                continue

            if segment.type == "set_expression":
                sub_segments = cls._retrieve_segments(segment)
                for sub_segment in  sub_segments:
                    if sub_segment.type == "select_statement":
                        select_statements.append(sub_segment)
                continue


            for conditional_handler in conditional_handlers:
                if conditional_handler.indicate(segment):
                    conditional_handler.handle(segment, holder)
        else:
            # call end of query hook here as loop is over
            for conditional_handler in conditional_handlers:
                conditional_handler.end_of_query_cleanup(holder)
        # By recursively extracting each subquery of the parent and merge, we're doing Depth-first search
        for sq in subqueries:
            holder |= cls._extract_from_dml_select(
                sq.segment, AnalyzerContext(sq, holder.cte)
            )
        for statement in select_statements:
            holder |= cls._extract_from_dml_select(
                statement, AnalyzerContext()
            )
        return holder

    @classmethod
    def parse_subquery(cls, segment: BaseSegment) -> List[SqlFluffSubQuery]:
        """The parse_subquery function takes a segment as an argument.

        Args:
            cls: Refer to the class, which is useful when you want to define a method that can be used as either a
                 static or instance method
            segment:BaseSegment: segment to determine if it is a subquery

        Returns:
            A list of `SqlFluffSubQuery` objects, otherwise, if the segment is not matching any of the expected types
            it returns an empty list.
        """
        result: List[SqlFluffSubQuery] = []
        identifiers = get_multiple_identifiers(segment)
        if identifiers and len(identifiers) > 1:
            # for SQL89 style of JOIN or multiple CTEs, this is actually SubQueries
            result = reduce(
                add,
                [
                    cls._parse_subquery(get_subqueries(identifier))
                    for identifier in identifiers
                ],
                [],
            )
        if segment.type in ["select_clause", "from_clause", "where_clause"]:
            result = cls._parse_subquery(get_subqueries(segment))
        elif is_subquery(segment):
            # Parenthesis for SubQuery without alias, this is valid syntax for certain SQL dialect
            result = [SqlFluffSubQuery.of(segment, None)]
        return result

    @classmethod
    def _parse_subquery(
        cls, sub_queries: List[SubSqlFluffQueryTuple]
    ) -> List[SqlFluffSubQuery]:
        """
        convert SubQueryTuple to sqllineage.core.models.SubQuery
        """
        return [
            SqlFluffSubQuery.of(bracketed_segment, alias)
            for bracketed_segment, alias in sub_queries
        ]

    @staticmethod
    def can_analyze(stmt: BaseSegment):
        """
        Check if the current lineage analyzer can analyze the statement

        :param stmt: a SQL base segment parsed by `sqlfluff`
        """
        return stmt.type in {
            "delete_statement",
            "truncate_table",
            "refresh_statement",
            "cache_table",
            "uncache_table",
            "show_statement",
            "drop_table_statement",
            "alter_table_statement",
            "rename_statement",
            "rename_table_statement",
            "select_statement",
            "insert_statement"
        }

    @staticmethod
    def _init_holder(context) -> SubQueryLineageHolder:
        holder = SubQueryLineageHolder()
        if context.prev_cte is not None:
            # CTE can be referenced by subsequent CTEs
            for cte in context.prev_cte:
                holder.add_cte(cte)
        if context.subquery is not None:
            # If within subquery, then manually add subquery as target table
            holder.add_write(context.subquery)
        return holder

    @staticmethod
    def _init_handlers() -> Tuple[
        List[SegmentBaseHandler], List[ConditionalSegmentBaseHandler]
    ]:
        handlers = [
            handler_cls() for handler_cls in SegmentBaseHandler.__subclasses__()
        ]
        conditional_handlers = [
            handler_cls()
            for handler_cls in ConditionalSegmentBaseHandler.__subclasses__()
        ]
        return handlers, conditional_handlers

    @staticmethod
    def _retrieve_segments(statement: BaseSegment) -> List[BaseSegment]:
        if statement.type == "bracketed":
            segments = [
                segment
                for segment in statement.iter_segments(
                    expanding=["expression"], pass_through=True
                )
            ]
            return [
                seg
                for segment in segments
                for seg in segment.segments
                if not is_segment_negligible(seg)
            ]
        else:
            return [seg for seg in statement.segments if not is_segment_negligible(seg)]
