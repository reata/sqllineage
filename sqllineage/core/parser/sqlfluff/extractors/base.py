from functools import reduce
from operator import add
from typing import List, Optional, Type

from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.models import SubQuery, Table
from sqllineage.core.parser.sqlfluff.models import SqlFluffSubQuery, SqlFluffTable
from sqllineage.core.parser.sqlfluff.utils import (
    is_subquery,
    list_subqueries,
)
from sqllineage.utils.entities import AnalyzerContext, SubQueryTuple


class BaseExtractor:
    """
    Abstract class implementation for extract 'SubQueryLineageHolder' from different statement types
    """

    SUPPORTED_STMT_TYPES: List[str] = []

    def __init__(self, dialect: str):
        self.dialect = dialect

    def can_extract(self, statement_type: str) -> bool:
        """
        Determine if the current lineage holder extractor can process the statement
        :param statement_type: a sqlfluff segment type
        """
        return statement_type in self.SUPPORTED_STMT_TYPES

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
        raise NotImplementedError

    @classmethod
    def find_table(cls, segment: BaseSegment) -> Optional[Table]:
        table = None
        if segment.type in ["table_reference", "object_reference"]:
            table = SqlFluffTable.of(segment)
        return table

    @classmethod
    def list_subquery(cls, segment: BaseSegment) -> List[SubQuery]:
        """
        The parse_subquery function takes a segment as an argument.
        :param segment: segment to determine if it is a subquery
        :return: A list of `SqlFluffSubQuery` objects, otherwise, if the segment is not matching any of the expected
        types it returns an empty list.
        """
        result: List[SubQuery] = []
        identifiers = segment.get_children("from_expression")
        if identifiers and len(identifiers) > 1:
            # for SQL89 style of JOIN or multiple CTEs, this is actually SubQueries
            return reduce(
                add,
                [
                    cls._parse_subquery(list_subqueries(identifier))
                    for identifier in identifiers
                ],
                [],
            )
        if segment.type in ["select_clause", "from_clause", "where_clause"]:
            result = cls._parse_subquery(list_subqueries(segment))
        elif is_subquery(segment):
            # Parenthesis for SubQuery without alias, this is valid syntax for certain SQL dialect
            result = [SqlFluffSubQuery.of(segment, None)]
        return result

    @classmethod
    def _parse_subquery(cls, subqueries: List[SubQueryTuple]) -> List[SubQuery]:
        """
        Convert a list of 'SqlFluffSubQueryTuple' to 'SqlFluffSubQuery'
        :param subqueries:  a list of 'SqlFluffSubQueryTuple'
        :return:  a list of 'SqlFluffSubQuery'
        """
        return [
            SqlFluffSubQuery.of(bracketed_segment, alias)
            for bracketed_segment, alias in subqueries
        ]

    def delegate_to(
        self,
        extractor_cls: "Type[BaseExtractor]",
        segment: BaseSegment,
        context: AnalyzerContext,
    ) -> SubQueryLineageHolder:
        """
        delegate to another type of extractor to extract
        """
        return extractor_cls(self.dialect).extract(segment, context)

    @staticmethod
    def _init_holder(context: AnalyzerContext) -> SubQueryLineageHolder:
        """
        Initialize lineage holder for a given 'AnalyzerContext'
        :param context: a previous context that the lineage extractor must consider
        :return: an initialized SubQueryLineageHolder
        """
        holder = SubQueryLineageHolder()

        if context.cte is not None:
            # CTE can be referenced by subsequent CTEs
            for cte in context.cte:
                holder.add_cte(cte)

        if context.write is not None:
            # If within subquery, then manually add subquery as target table
            for write in context.write:
                holder.add_write(write)

        if context.write_columns:
            # target columns can be referred while creating column level lineage
            holder.add_write_column(*context.write_columns)

        return holder
