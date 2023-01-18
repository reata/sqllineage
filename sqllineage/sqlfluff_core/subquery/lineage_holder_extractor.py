from abc import ABC, abstractmethod
from functools import reduce
from operator import add
from typing import List, Tuple

from sqlfluff.core.parser import BaseSegment

from sqllineage.sqlfluff_core.handlers.base import (
    ConditionalSegmentBaseHandler,
    SegmentBaseHandler,
)
from sqllineage.sqlfluff_core.holders import SqlFluffSubQueryLineageHolder
from sqllineage.sqlfluff_core.models import SqlFluffAnalyzerContext
from sqllineage.sqlfluff_core.models import SqlFluffSubQuery
from sqllineage.sqlfluff_core.utils.entities import SubSqlFluffQueryTuple
from sqllineage.sqlfluff_core.utils.sqlfluff import (
    get_multiple_identifiers,
    get_subqueries,
    is_subquery,
)


class LineageHolderExtractor(ABC):
    """
    Abstract class implementation for extract 'SqlFluffSubQueryLineageHolder' from different statement types
    """

    def __init__(self, dialect: str):
        self.dialect = dialect

    @abstractmethod
    def can_extract(self, statement_type: str) -> bool:
        """
        Determine if the current lineage holder extractor can process the statement
        :param statement_type: a sqlfluff segment type
        """
        pass

    @abstractmethod
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
        pass

    @classmethod
    def parse_subquery(cls, segment: BaseSegment) -> List[SqlFluffSubQuery]:
        """
        The parse_subquery function takes a segment as an argument.
        :param segment: segment to determine if it is a subquery
        :return: A list of `SqlFluffSubQuery` objects, otherwise, if the segment is not matching any of the expected
        types it returns an empty list.
        """
        result: List[SqlFluffSubQuery] = []
        identifiers = get_multiple_identifiers(segment)
        if identifiers and len(identifiers) > 1:
            # for SQL89 style of JOIN or multiple CTEs, this is actually SubQueries
            return reduce(
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
        cls, subqueries: List[SubSqlFluffQueryTuple]
    ) -> List[SqlFluffSubQuery]:
        """
        Convert a list of 'SqlFluffSubQueryTuple' to 'SqlFluffSubQuery'
        :param subqueries:  a list of 'SqlFluffSubQueryTuple'
        :return:  a list of 'SqlFluffSubQuery'
        """
        return [
            SqlFluffSubQuery.of(bracketed_segment, alias)
            for bracketed_segment, alias in subqueries
        ]

    def _init_handlers(
        self,
    ) -> Tuple[List[SegmentBaseHandler], List[ConditionalSegmentBaseHandler]]:
        """
        Initialize handlers used during the extraction of lineage
        :return: A tuple with a list of SegmentBaseHandler and ConditionalSegmentBaseHandler
        """
        handlers = [
            handler_cls() for handler_cls in SegmentBaseHandler.__subclasses__()
        ]
        conditional_handlers = [
            handler_cls(self.dialect)
            for handler_cls in ConditionalSegmentBaseHandler.__subclasses__()
        ]
        return handlers, conditional_handlers

    @staticmethod
    def _init_holder(context: SqlFluffAnalyzerContext) -> SqlFluffSubQueryLineageHolder:
        """
        Initialize lineage holder for a given 'SqlFluffAnalyzerContext'
        :param context: a previous context that the lineage extractor must consider
        :return: an initialized SqlFluffSubQueryLineageHolder
        """
        holder = SqlFluffSubQueryLineageHolder()

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
