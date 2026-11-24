from sqlfluff.core.parser import BaseSegment

from sqllineage.core.models import (
    Column,
    LateralView,
    Schema,
    SourcePosition,
    SubQuery,
    Table,
)
from sqllineage.core.parser.sqlfluff.utils import (
    extract_column_and_alias,
    extract_column_qualifier,
    extract_identifier,
    extract_source_columns,
    extract_wildcard_except_columns,
    is_teradata_title_phrase,
    is_wildcard,
    list_alias_expression_column_aliases,
    list_child_segments,
)
from sqllineage.utils.entities import ColumnQualifierTuple
from sqllineage.utils.helpers import escape_identifier_name


class SqlFluffTable(Table):
    """
    Data Class for SqlFluffTable
    """

    @staticmethod
    def of(
        table: BaseSegment, alias: str | None = None, dialect: str = "ansi"
    ) -> Table:
        """
        Build an object of type 'Table'
        :param table: table segment to be processed
        :param alias: alias of the table segment
        :param dialect: sql dialect
        :return: 'Table' object
        """
        dot_idx = None
        for idx in range(len(table.segments) - 2, -1, -1):
            token = table.segments[idx]
            if bool(token.type == "symbol"):
                dot_idx, _ = idx, token
                break
        real_name = escape_identifier_name(
            table.segments[dot_idx + 1].raw
            if dot_idx
            else (table.raw if table.type == "identifier" else table.segments[0].raw)
        )
        # rewrite identifier's get_parent_name accordingly
        parent_name = (
            "".join(
                [
                    escape_identifier_name(segment.raw)
                    for segment in table.segments[:dot_idx]
                ]
            )
            if dot_idx
            else None
        )
        schema = Schema(parent_name) if parent_name is not None else Schema()
        kwargs = {"alias": alias} if alias else {}
        if dot_idx is None:
            # no dot-symbol found, dots are inside a quoted identifier
            # for most dialects dots are literal characters in the name;
            # BigQuery is the exception — backtick-quoted dots remain separators
            escaped = dialect != "bigquery"
        else:
            # dot-symbol found, .of() already decomposed schema and table name
            escaped = True
        return Table(real_name, schema, escaped=escaped, **kwargs)


class SqlFluffSubQuery(SubQuery):
    """
    Data Class for SqlFluffSubQuery
    """

    @staticmethod
    def of(subquery: BaseSegment, alias: str | None) -> SubQuery:
        """
        Build a 'SubQuery' object
        :param subquery: subquery segment
        :param alias: subquery alias
        :return: 'SubQuery' object
        """
        position = SourcePosition(-1, -1)
        if pos_marker := subquery.pos_marker:
            position = SourcePosition(pos_marker.line_no, pos_marker.line_pos)
        return SubQuery(subquery, subquery.raw, alias, position=position)


class SqlFluffLateralView(LateralView):
    """
    Data Class for SqlFluffLateralView
    """

    @staticmethod
    def of(subquery: BaseSegment, alias: str | None = None) -> LateralView:
        """
        Build a 'LateralView' object
        :param subquery: lateral_view_clause segment, or from_expression_element segment of a table function
        :param alias: lateral view alias, inferred from the segment if not given
        :return: 'LateralView' object
        """
        position = SourcePosition(-1, -1)
        if pos_marker := subquery.pos_marker:
            position = SourcePosition(pos_marker.line_no, pos_marker.line_pos)
        if subquery.type == "from_expression_element":
            return SqlFluffLateralView._of_table_function(subquery, alias, position)
        # LATERAL VIEW explode(col1) alias AS col2, col3: the identifier before AS is the table alias and the ones
        # after it are the column aliases. The table alias can be omitted (LATERAL VIEW explode(col1) AS col2), and
        # sparksql also accepts the column aliases without AS, in which case the first identifier is the table alias.
        # databricks parses AS as an identifier when the table alias is omitted, so check the raw text.
        before_as: list[str] = []
        after_as: list[str] | None = None
        for seg in list_child_segments(subquery):
            if seg.type in ("keyword", "identifier") and seg.raw_upper == "AS":
                after_as = []
            elif seg.type == "identifier":
                (before_as if after_as is None else after_as).append(str(seg.raw))
        if alias is None and before_as:
            alias = before_as[0]
        function = subquery.get_child("function")
        return LateralView(
            subquery,
            subquery.raw,
            alias,
            position=position,
            generator=SqlFluffColumn.of(function) if function else None,
            column_names=before_as[1:] if after_as is None else after_as,
        )

    @staticmethod
    def _of_table_function(
        segment: BaseSegment, alias: str | None, position: SourcePosition
    ) -> LateralView:
        """
        Build a 'LateralView' object for a table function in FROM clause, like UNNEST in presto/trino/bigquery/postgres
        or FLATTEN in snowflake:
            FROM tab CROSS JOIN UNNEST(tab.col1) [WITH ORDINALITY] AS alias (col2, col3)
        The identifier after AS is the table alias and the optional bracketed list following it are the column aliases
        :param segment: from_expression_element segment whose table_expression is a function
        """
        function = None
        if table_expression := segment.get_child("table_expression"):
            function = table_expression.get_child("function")
        column_names: list[str] = []
        if alias_expression := segment.get_child("alias_expression"):
            if alias is None:
                alias = extract_identifier(alias_expression)
            column_names = [
                str(identifier.raw)
                for identifier in list_alias_expression_column_aliases(alias_expression)
            ]
        return LateralView(
            segment,
            segment.raw,
            alias,
            position=position,
            generator=SqlFluffColumn.of(function) if function else None,
            column_names=column_names,
            # bigquery UNNEST(tab.col1) AS col2 or postgres unnest(tab.col1) AS col2 names the generated column col2
            alias_as_column=True,
        )


class SqlFluffColumn(Column):
    """
    Data Class for SqlFluffColumn
    """

    @staticmethod
    def of(column: BaseSegment, **kwargs) -> Column:
        """
        Build a 'Column' object
        :param column: column segment
        :return: 'Column' object
        """
        if column.type == "select_clause_element":

            # Special handling for Teradata TITLE phrase
            if is_teradata_title_phrase(column):
                function_name_identifier = next(
                    column.recursive_crawl("function_name_identifier")
                )
                return Column(
                    function_name_identifier.raw,
                    source_columns=[
                        ColumnQualifierTuple(function_name_identifier.raw, None)
                    ],
                )

            source_columns, alias = extract_column_and_alias(column)
            if alias:
                return Column(alias, source_columns=source_columns, from_alias=True)
            if source_columns:
                column_name = None
                except_columns: list[str] = []
                for sub_segment in list_child_segments(column):
                    if sub_segment.type == "column_reference" or is_wildcard(
                        sub_segment
                    ):
                        if cqt := extract_column_qualifier(sub_segment):
                            column_name = cqt.column
                        if is_wildcard(sub_segment):
                            except_columns = extract_wildcard_except_columns(
                                sub_segment
                            )
                    elif sub_segment.type == "expression":
                        # special handling for postgres style type cast, col as target column name instead of col::type
                        if len(sub2_segments := list_child_segments(sub_segment)) == 1:
                            if (
                                sub2_segment := sub2_segments[0]
                            ).type == "cast_expression":
                                if (
                                    len(
                                        sub3_segments := list_child_segments(
                                            sub2_segment
                                        )
                                    )
                                    == 2
                                ):
                                    if (
                                        sub3_segment := sub3_segments[0]
                                    ).type == "column_reference":
                                        if cqt := extract_column_qualifier(
                                            sub3_segment
                                        ):
                                            column_name = cqt.column
                return Column(
                    column.raw if column_name is None else column_name,
                    source_columns=source_columns,
                    except_columns=except_columns,
                )

        # Wildcard, Case, Function without alias (thus not recognized as an Identifier)
        source_columns = extract_source_columns(column)
        return Column(column.raw, source_columns=source_columns)
