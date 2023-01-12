from abc import ABC, abstractmethod
from functools import reduce
from operator import add
from typing import Tuple, List, Optional

from sqlfluff.core.parser import BaseSegment

from sqllineage.sqlfluff_core.models import SqlFluffAnalyzerContext

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.sqlfluff_core.handlers.base import (
    SegmentBaseHandler,
    ConditionalSegmentBaseHandler,
)
from sqllineage.sqlfluff_core.models import SqlFluffSubQuery
from sqllineage.sqlfluff_core.utils.entities import SubSqlFluffQueryTuple
from sqllineage.sqlfluff_core.utils.sqlfluff import (
    get_multiple_identifiers,
    get_sub_queries,
    is_subquery,
)


class LineageHolderExtractor(ABC):
    def __init__(self, dialect: str):
        self.dialect = dialect

    @abstractmethod
    def can_extract(self, statement_type: str):
        pass

    @abstractmethod
    def extract(
        self,
        statement: BaseSegment,
        context: SqlFluffAnalyzerContext,
        is_sub_query: bool = False,
    ) -> Optional[SubQueryLineageHolder]:
        pass

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
            return reduce(
                add,
                [
                    cls._parse_subquery(get_sub_queries(identifier))
                    for identifier in identifiers
                ],
                [],
            )
        if segment.type in ["select_clause", "from_clause", "where_clause"]:
            result = cls._parse_subquery(get_sub_queries(segment))
        elif is_subquery(segment):
            # Parenthesis for SubQuery without alias, this is valid syntax for certain SQL dialect
            result = [SqlFluffSubQuery.of(segment, None)]
        return result

    @classmethod
    def _parse_subquery(
        cls, sub_queries: List[SubSqlFluffQueryTuple]
    ) -> List[SqlFluffSubQuery]:
        """
        Convert SubQueryTuple to sqllineage.core.models.SubQuery
        """
        return [
            SqlFluffSubQuery.of(bracketed_segment, alias)
            for bracketed_segment, alias in sub_queries
        ]

    def _init_handlers(
        self,
    ) -> Tuple[List[SegmentBaseHandler], List[ConditionalSegmentBaseHandler]]:
        handlers = [
            handler_cls() for handler_cls in SegmentBaseHandler.__subclasses__()
        ]
        conditional_handlers = [
            handler_cls(self.dialect)
            for handler_cls in ConditionalSegmentBaseHandler.__subclasses__()
        ]
        return handlers, conditional_handlers

    @staticmethod
    def _init_holder(context) -> SubQueryLineageHolder:
        holder = SubQueryLineageHolder()

        if context.prev_cte is not None:
            # CTE can be referenced by subsequent CTEs
            for cte in context.prev_cte:
                holder.add_cte(cte)

        if context.prev_write is not None:
            # If within subquery, then manually add subquery as target table
            for write in context.prev_write:
                holder.add_write(write)

        if context.prev_write is None and context.subquery is not None:
            # If within subquery, then manually add subquery as target table
            holder.add_write(context.subquery)

        return holder
