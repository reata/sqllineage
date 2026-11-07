from sqlfluff.core.parser import BaseSegment

from sqllineage.core.holders import SubQueryLineageHolder
from sqllineage.core.models import Column, Table
from sqllineage.core.parser.sqlfluff.extractors.base import BaseExtractor
from sqllineage.core.parser.sqlfluff.extractors.select import SelectExtractor
from sqllineage.core.parser.sqlfluff.models import (
    SqlFluffColumn,
    SqlFluffSubQuery,
    SqlFluffTable,
)
from sqllineage.core.parser.sqlfluff.utils import (
    extract_column_qualifier,
    is_negligible,
    list_child_segments,
)
from sqllineage.utils.entities import AnalyzerContext


class MultiTableInsertExtractor(BaseExtractor):
    """
    Multi-table insert statement lineage extractor, for snowflake and oracle only:

    .. code-block:: sql

        INSERT ALL
            INTO tab1 (col1, col2) VALUES (col3, col4)
            INTO tab2
        SELECT col3, col4 FROM tab3

    The query is read like a subquery. Each INTO clause writes the subquery columns listed in its VALUES clause,
    or all subquery columns by position when there is no VALUES clause, to its own table.

    sqlfluff parses INSERT {ALL | FIRST} as a plain insert_statement for both dialects, so this extractor takes no part
    in the statement type based dispatch of BaseExtractor.try_extract: SUPPORTED_STMT_TYPES is empty on purpose, and
    CreateInsertExtractor explicitly delegates to it when is_multi_table_insert() holds.
    """

    SUPPORTED_STMT_TYPES: list[str] = []

    @staticmethod
    def is_multi_table_insert(statement: BaseSegment) -> bool:
        """
        INSERT ALL or INSERT FIRST, followed by one or more INTO clauses
        """
        if statement.type == "insert_statement":
            for segment in statement.get_children("keyword"):
                if segment.raw_upper == "INTO":
                    break
                if segment.raw_upper in ("ALL", "FIRST"):
                    return True
        return False

    def extract(
        self,
        statement: BaseSegment,
        context: AnalyzerContext,
    ) -> SubQueryLineageHolder:
        holder = self._init_holder(context)
        # each INTO clause as: target table, target column list, VALUES expressions (empty without VALUES clause)
        into_clauses: list[tuple[Table, list[Column], list[BaseSegment]]] = []
        query = None
        into_flag = False
        for segment in list_child_segments(statement):
            if segment.type == "keyword":
                into_flag = segment.raw_upper == "INTO"
                continue
            if segment.type in (
                "select_statement",
                "set_expression",
                "with_compound_statement",
            ):
                query = segment
            elif into_flag and segment.type == "table_reference":
                table = SqlFluffTable.of(segment, dialect=self.dialect)
                holder.add_write(table)
                into_clauses.append((table, [], []))
            elif into_clauses and segment.type == "bracketed":
                into_clauses[-1][1].extend(
                    SqlFluffColumn.of(column_reference)
                    for column_reference in segment.get_children("column_reference")
                )
            elif into_clauses and segment.type == "values_clause":
                if bracketed := segment.get_child("bracketed"):
                    into_clauses[-1][2].extend(
                        s
                        for s in bracketed.segments
                        if not is_negligible(s)
                        and s.type not in ("start_bracket", "end_bracket", "comma")
                    )
            into_flag = False

        if query is not None:
            # CteExtractor imports CreateInsertExtractor, which imports this module
            from .cte import CteExtractor

            subquery = SqlFluffSubQuery.of(query, None)
            subquery_holder = self.delegate_to(
                (
                    CteExtractor
                    if query.type == "with_compound_statement"
                    else SelectExtractor
                ),
                query,
                AnalyzerContext(cte=holder.cte, write={subquery}),
            )
            holder |= subquery_holder
            for table, columns, values in into_clauses:
                # the source columns of each target column, in the order they're written to the table
                src_cols_list: list[list[Column]] = []
                default_names = []
                if values:
                    for value in values:
                        value_column = SqlFluffColumn.of(value)
                        src_cols = []
                        for src_col_name, _ in value_column.source_columns:
                            src_col = Column(src_col_name, escaped=True)
                            src_col.parent = subquery
                            src_cols.append(src_col)
                        src_cols_list.append(src_cols)
                        # a column reference is written to the column of the same name, like INSERT INTO ... SELECT
                        if (
                            value.type == "expression"
                            and len(children := list_child_segments(value)) == 1
                        ):
                            value = children[0]
                        cqt = (
                            extract_column_qualifier(value)
                            if value.type == "column_reference"
                            else None
                        )
                        default_names.append(
                            cqt.column if cqt else value_column.raw_name
                        )
                else:
                    for query_column in subquery_holder.write_columns:
                        src_cols_list.append([query_column])
                        default_names.append(query_column.raw_name)
                if len(columns) != len(src_cols_list) and self.metadata_provider:
                    columns = self.metadata_provider.get_table_columns(table)
                if len(columns) != len(src_cols_list):
                    columns = [Column(name, escaped=True) for name in default_names]
                for tgt_col, src_cols in zip(columns, src_cols_list):
                    tgt_col.parent = table
                    for src_col in src_cols:
                        holder.add_column_lineage(src_col, tgt_col)
                holder.expand_wildcard(self.metadata_provider, table)
        return holder
