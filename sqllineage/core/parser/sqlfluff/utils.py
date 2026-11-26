"""
Utils class to deal with the sqlfluff segments manipulations

naming convention:
    is_x: BaseSegment -> bool
    find_x: BaseSegment -> Optional[BaseSegment]
    list_x: BaseSegment -> list[BaseSegment]
    extract_x: other pattern
first parameter of each function must be sqlfluff BaseSegment
"""

from sqlfluff.core.parser import BaseSegment

from sqllineage import SQLPARSE_DIALECT
from sqllineage.core.models import Table
from sqllineage.utils.entities import ColumnQualifierTuple, SubQueryTuple

NON_IDENTIFIER_OR_COLUMN_SEGMENT_TYPE = [
    "partitionby_clause",
    "orderby_clause",
    "expression",
    "case_expression",
    "when_clause",
    "else_clause",
    "select_clause_element",
    "cast_expression",
    # snowflake named argument, like FLATTEN(input => tab.col1)
    "snowflake_keyword_expression",
    # array constructor, like ARRAY[col1, col2] or [col1, col2]
    "typed_array_literal",
    "array_literal",
    # flink ARRAY[col1, col2]
    "array_expression",
    # struct constructor, like STRUCT(col1 AS a, col2) or STRUCT<a INT64>(col1)
    "typed_struct_literal",
    "struct_literal",
    # struct/object constructor, like {'a': col1, 'b': col2}
    "object_literal",
    "object_literal_element",
    # named argument, like struct_pack(a := col1) or make_interval(days => col1)
    "named_argument",
]

# the first child of these is a key or argument name, not a source column
KEY_VALUE_SEGMENT_TYPE = ["object_literal_element", "named_argument"]

FUNCTION_SEGMENT_TYPE = ["function"]

COLUMN_SEGMENT_TYPE = ["identifier", "column_reference"]

SOURCE_COLUMN_SEGMENT_TYPE = (
    NON_IDENTIFIER_OR_COLUMN_SEGMENT_TYPE + FUNCTION_SEGMENT_TYPE + COLUMN_SEGMENT_TYPE
)


def is_negligible(segment: BaseSegment) -> bool:
    return (
        segment.is_whitespace
        or segment.is_comment
        or bool(segment.is_meta)
        or (segment.type == "symbol" and segment.raw != "*")
    )


def is_set_expression(segment: BaseSegment) -> bool:
    return segment.type == "set_expression" or any(
        seg.type == "set_expression" for seg in segment.segments
    )


def is_subquery(segment: BaseSegment) -> bool:
    if segment.type in ["from_expression_element", "bracketed"]:
        token = extract_innermost_bracketed(
            segment if segment.type == "bracketed" else segment.segments[0]
        )
        # check if innermost parenthesis contains SELECT
        if token.get_child(
            "select_statement", "set_expression", "with_compound_statement"
        ):
            return True
        elif expression := token.get_child("expression"):
            if expression.get_child("select_statement"):
                return True
    return False


def is_wildcard(segment: BaseSegment) -> bool:
    return segment.type == "wildcard_expression" or (
        segment.type == "symbol" and segment.raw == "*" and segment.get_type() == "star"
    )


def is_teradata_title_phrase(segment: BaseSegment) -> bool:
    """
    Check if a select_clause_element contains Teradata specific TITLE phrase
    Teradata TITLE phrase syntax: column_name (TITLE 'title_name')
    This gets incorrectly parsed as a function by sqlfluff
    """
    if segment.type == "select_clause_element":
        if function := segment.get_child("function"):
            if function_contents := function.get_child("function_contents"):
                if bracketed := function_contents.get_child("bracketed"):
                    if expression := bracketed.get_child("expression"):
                        if data_type := expression.get_child("data_type"):
                            if data_type_identifier := data_type.get_child(
                                "data_type_identifier"
                            ):
                                if data_type_identifier.raw_upper == "TITLE":
                                    return True
    return False


def find_from_expression_element(segment: BaseSegment) -> BaseSegment | None:
    """
    segment can be of type:
        from_clause as grandparent
        from_expression/join_clause as parent
    """
    try:
        from_expression_element = next(
            segment.recursive_crawl("from_expression_element")
        )
    except StopIteration:
        from_expression_element = None
    return from_expression_element


def find_table_identifier(segment: BaseSegment) -> BaseSegment | None:
    """
    recursively find table identifier
    """
    table_identifier = None
    if segment.type in ["table_reference", "file_reference", "object_reference"]:
        # snowflake: SELECT $1 FROM @mystage/path/,
        # the stage_path is wrapped in table_reference
        return segment.get_child("stage_path") or segment
    elif segment.is_type("stage_path"):
        # snowflake: SELECT t.$1 FROM @mystage/path/ (FILE_FORMAT => 'myformat') t
        # the stage_path followed by an option list
        # is a direct child of table_expression
        return segment
    else:
        for sub_segment in segment.segments:
            if identifier := find_table_identifier(sub_segment):
                return identifier
    return table_identifier


def list_join_clause(segment: BaseSegment) -> list[BaseSegment]:
    """
    traverse from_clause, recursively goes into bracket by default
    """
    if segment.type in ["from_clause", "update_statement"]:
        # for select from subquery, do not recursively go into subquery
        if from_expression := segment.get_child("from_expression"):
            join_clause = from_expression.get_child("join_clause")
            if not join_clause:
                try:
                    next(from_expression.recursive_crawl("select_clause"))
                except StopIteration:
                    pass
                else:
                    # no join at top level, and there is select statement in from_clause
                    return []
        # otherwise, recursively find join_clause
        return list(segment.recursive_crawl("join_clause"))
    return []


def list_expression_from_when_clause(
    when_clause: BaseSegment, sub_type: str
) -> list[BaseSegment]:
    """
    sub_type can be either WHEN or THEN, this is to extract subquery for case when
    (SELECT ...) then (SELECT ...)
    """
    segments = []
    start_flag = False
    from_keyword, end_keyword = sub_type, "THEN" if sub_type == "WHEN" else None
    for segment in when_clause.segments:
        if segment.type == "keyword" and segment.raw_upper == from_keyword:
            start_flag = True
            continue
        if (
            end_keyword is not None
            and segment.type == "keyword"
            and segment.raw_upper == end_keyword
        ):
            break
        if start_flag and segment.type == "expression":
            segments = segment.get_children("bracketed")
    return segments


def list_subqueries(segment: BaseSegment) -> list[SubQueryTuple]:
    subquery = []
    match segment.type:
        case "select_clause":
            for select_clause_element in segment.get_children("select_clause_element"):
                if expression := select_clause_element.get_child("expression"):
                    if case_expression := expression.get_child("case_expression"):
                        for when_clause in case_expression.get_children("when_clause"):
                            for bracketed_segment in list_expression_from_when_clause(
                                when_clause, sub_type="WHEN"
                            ):
                                subquery.append(SubQueryTuple(bracketed_segment, None))
                            for bracketed_segment in list_expression_from_when_clause(
                                when_clause, sub_type="THEN"
                            ):
                                alias_expression = select_clause_element.get_child(
                                    "alias_expression"
                                )
                                alias = (
                                    extract_identifier(alias_expression)
                                    if alias_expression
                                    else None
                                )
                                subquery.append(SubQueryTuple(bracketed_segment, alias))
                        # subquery living in the ELSE branch of a CASE expression
                        if else_clause := case_expression.get_child("else_clause"):
                            if else_expression := else_clause.get_child("expression"):
                                for bracketed in else_expression.get_children(
                                    "bracketed"
                                ):
                                    if is_subquery(bracketed):
                                        subquery.append(
                                            SubQueryTuple(
                                                extract_innermost_bracketed(bracketed),
                                                None,
                                            )
                                        )
                    else:
                        # scalar subquery used directly as a projected column,
                        # e.g. SELECT (SELECT max(x) FROM b) AS m FROM a
                        for bracketed in expression.get_children("bracketed"):
                            if is_subquery(bracketed):
                                alias_expression = select_clause_element.get_child(
                                    "alias_expression"
                                )
                                alias = (
                                    extract_identifier(alias_expression)
                                    if alias_expression
                                    else None
                                )
                                subquery.append(
                                    SubQueryTuple(
                                        extract_innermost_bracketed(bracketed), alias
                                    )
                                )
                elif function := select_clause_element.get_child("function"):
                    for bracketed in function.recursive_crawl("bracketed"):
                        if is_subquery(bracketed):
                            subquery.append(SubQueryTuple(bracketed, None))
        case "from_expression_element":
            target = extract_target_segment(segment)
            if is_subquery(target):
                subquery = [
                    SubQueryTuple(
                        (
                            extract_innermost_bracketed(target)
                            if not is_set_expression(target)
                            else target
                        ),
                        extract_from_expression_element_alias(segment),
                    )
                ]
        case "where_clause":
            bracketeds = []
            if expression := segment.get_child("expression"):
                bracketeds = expression.get_children("bracketed")
            elif bracketed_where := segment.get_child("bracketed"):
                if expression := bracketed_where.get_child("expression"):
                    bracketeds = expression.get_children("bracketed")
            subquery = [
                SubQueryTuple(extract_innermost_bracketed(bracketed), None)
                for bracketed in bracketeds
                if is_subquery(bracketed)
            ]
        case "from_clause" | "from_expression":
            if from_expression_element := find_from_expression_element(segment):
                subquery = list_subqueries(from_expression_element)
            for join_clause in list_join_clause(segment):
                if from_expression_element := find_from_expression_element(join_clause):
                    subquery += list_subqueries(from_expression_element)
    return subquery


def list_subquery_column_aliases(segment: BaseSegment) -> list[BaseSegment]:
    """
    For a subquery segment, list the identifiers of the column alias list that renames
    its output columns:
        FROM (SELECT ...) AS alias (col1, col2)
        WITH alias (col1, col2) AS (SELECT ...)
    Return an empty list when no column alias list is specified.
    """
    parent = segment.get_parent()
    # the subquery may be wrapped in extra parenthesis
    # like: FROM ((SELECT ...)) AS alias (col1, col2)
    while parent is not None and parent[0].type in ["bracketed", "table_expression"]:
        parent = parent[0].get_parent()
    if parent is not None:
        if parent[0].type == "from_expression_element":
            if alias_expression := parent[0].get_child("alias_expression"):
                return list_alias_expression_column_aliases(alias_expression)
        elif parent[0].type == "common_table_expression":
            if cte_column_list := parent[0].get_child("cte_column_list"):
                return list_alias_expression_column_aliases(cte_column_list)
    return []


def list_alias_expression_column_aliases(segment: BaseSegment) -> list[BaseSegment]:
    """
    For an alias_expression or cte_column_list segment, list the identifiers of its
    bracketed column alias list:
        AS alias (col1, col2)
        WITH alias (col1, col2) AS (...)
    Return an empty list when no column alias list is specified.
    """
    # tsql wraps the bracketed column alias list in alias_column_list
    column_alias_list = segment.get_child("alias_column_list") or segment
    if bracketed := column_alias_list.get_child("bracketed"):
        if identifier_list := bracketed.get_child("identifier_list"):
            return identifier_list.get_children("identifier")
    return []


def extract_alias_and_column_names(
    segment: BaseSegment,
) -> tuple[str | None, list[str]]:
    """
    For a from_expression_element segment, extract its alias and the names in its column
    alias list:
        FROM UNNEST(tab.col1) AS alias (col1, col2)
        FROM (VALUES (1, 2)) AS alias (col1, col2)
    Return (None, []) without alias, and an empty list when no column alias list is
    specified.
    """
    if alias_expression := segment.get_child("alias_expression"):
        column_names = [
            str(identifier.raw)
            for identifier in list_alias_expression_column_aliases(alias_expression)
        ]
        return extract_identifier(alias_expression), column_names
    return None, []


def list_lateral_view_clauses(segment: BaseSegment) -> list[BaseSegment]:
    """
    For a from_clause segment, list the hive/sparksql LATERAL VIEW clauses:
        FROM tab LATERAL VIEW explode(tab.col1) alias AS col2
    hive puts lateral_view_clause under from_expression_element, while sparksql puts it
    under from_expression
    """
    lateral_view_clauses = []
    if segment.type == "from_clause":
        for from_expression in segment.get_children("from_expression"):
            for seg in [
                from_expression,
                *from_expression.get_children("from_expression_element"),
            ]:
                lateral_view_clauses += seg.get_children("lateral_view_clause")
    return lateral_view_clauses


def list_child_segments(
    segment: BaseSegment, check_bracketed: bool = True
) -> list[BaseSegment]:
    """
    Filter segments for a given segment's children, recursive goes into bracket by
    default
    """
    if segment.type == "bracketed" and check_bracketed:
        if is_set_expression(segment):
            return [seg for seg in segment.segments if seg.type == "set_expression"]
        else:
            result = []
            for seg in segment.iter_segments(
                expanding=["expression"], pass_through=True
            ):
                if seg.type in ["column_reference", "column_definition"]:
                    result.append(seg)
                else:
                    for s in seg.segments:
                        if not is_negligible(s):
                            result.append(s)
            return result
    else:
        return [seg for seg in segment.segments if not is_negligible(seg)]


def extract_identifier(col_segment: BaseSegment) -> str:
    identifiers = list_child_segments(col_segment)

    # tsql assignment operator syntax: alias_name = expression
    # starting from sqlfluff 3.4.2, the alias_expression
    # contains the alias operator that we need to get rid of
    if col_segment.type == "alias_expression" and col_segment.get_child(
        "alias_operator"
    ):
        identifiers = [seg for seg in identifiers if seg.type != "alias_operator"]

    # alias followed by a column alias list: (SELECT ...) AS alias (col1, col2)
    # tsql wraps the bracketed column alias list in alias_column_list
    if col_segment.type == "alias_expression":
        identifiers = [
            seg
            for seg in identifiers
            if seg.type not in ["bracketed", "alias_column_list"]
        ]

    col_identifier = identifiers[-1]
    return str(col_identifier.raw)


def extract_target_segment(segment: BaseSegment) -> BaseSegment:
    sublist = list_child_segments(segment, False)
    target = sublist[0]
    if target.type == "keyword" and target.raw_upper == "LATERAL":
        target = sublist[1]
    table_expr = target if is_subquery(target) else target.segments[0]
    return table_expr


def extract_from_expression_element_alias(segment: BaseSegment) -> str | None:
    """
    For a from_expression_element segment, extract the alias the query refers to its
    dataset by:
        FROM tab AS alias
        FROM (SELECT ...) AS alias
        FROM ((tab AS alias JOIN ...))
        FROM tab PIVOT (sum(col1) FOR col2 IN ('a', 'b')) AS alias
    An alias after table operators like PIVOT/UNPIVOT names their result, so it takes
    precedence over the alias before the operators. Return None when no alias is
    specified.
    """
    if alias := _extract_table_operator_alias(segment):
        return alias
    alias_expression = segment.get_child("alias_expression")
    if alias_expression is None:
        child_segments = [
            seg for seg in list_child_segments(segment) if seg.type != "keyword"
        ]
        if (
            len(child_segments) == 1
            and child_segments[0].type == "bracketed"
            and not is_subquery(child_segments[0])
        ):
            # the alias_expression may be deeply nested in the bracketed segment
            alias_expression = next(
                child_segments[0].recursive_crawl("alias_expression"), None
            )
    return extract_identifier(alias_expression) if alias_expression else None


def _extract_table_operator_alias(segment: BaseSegment) -> str | None:
    """
    For a from_expression_element segment, find the alias of the table operators applied
    to its dataset:
        FROM tab PIVOT (sum(col1) FOR col2 IN ('a', 'b')) AS alias
        FROM tab UNPIVOT (col1 FOR col2 IN (col3, col4)) AS alias
        FROM tab AT (TIMESTAMP => '2024-01-01'::timestamp) AS alias
    sqlfluff puts the operators either inside the from_expression_element, or after it
    under from_expression.
    tsql puts the alias inside the PIVOT/UNPIVOT operator.
    Return None when no aliased operator is applied.
    """
    segments = list_child_segments(segment)[1:]
    if (parent := segment.get_parent()) is not None:
        parent_segment, idx = parent
        if parent_segment.type in ["from_expression", "join_clause"]:
            segments += [
                seg
                for seg in parent_segment.segments[idx + 1 :]
                if not is_negligible(seg)
            ]
    alias = None
    operator_applied = False
    for seg in segments:
        if seg.type in [
            "from_pivot_expression",
            "from_unpivot_expression",
            "pivot_clause",
            "unpivot_clause",
            "from_at_expression",
            "from_before_expression",
            "changes_clause",
        ]:
            operator_applied = True
            if table_reference := seg.get_child("table_reference"):
                alias = extract_identifier(table_reference)
        elif seg.type == "alias_expression":
            # only an alias after the operators names their result
            if operator_applied:
                alias = extract_identifier(seg)
        else:
            break
    return alias


def extract_column_qualifier(segment: BaseSegment) -> ColumnQualifierTuple | None:
    cqt = None
    if is_wildcard(segment):
        # a wildcard_expression like "t.* EXCEPT(col)" also contains the EXCEPT/REPLACE
        # clause; only wildcard_identifier (e.g. "t.*") identifies the column/qualifier
        wildcard_segment = segment.get_child("wildcard_identifier") or segment
        identifiers = wildcard_segment.raw.split(".")
        column = identifiers[-1]
        parent = identifiers[-2] if len(identifiers) > 1 else None
        cqt = ColumnQualifierTuple(column, parent)
    else:
        match segment.type:
            case "column_reference":
                sub_segments = list_child_segments(segment)
                column = sub_segments[-1].raw
                parent = sub_segments[-2].raw if len(sub_segments) > 1 else None
                cqt = ColumnQualifierTuple(column, parent)
            case "identifier":
                cqt = ColumnQualifierTuple(segment.raw, None)
    return cqt


def extract_wildcard_except_columns(wildcard_expression: BaseSegment) -> list[str]:
    """
    For a wildcard_expression segment, return the column names it leaves out with
    "* EXCEPT (col1, col2)" (BigQuery, ClickHouse, Databricks, SparkSQL) or
    "* EXCLUDE (col1, col2)" (DuckDB, Snowflake, Redshift). EXCLUDE also accepts a
    single unbracketed column, like "* EXCLUDE col1". Returns an empty list when the
    wildcard has no such clause.
    """
    wildcard_except_segment_types = (
        "select_except_clause",
        "select_exclude_clause",
        "wildcard_exclude",
    )
    wildcard_except_segment = wildcard_expression.get_children(
        *wildcard_except_segment_types
    )
    if not wildcard_except_segment:
        # redshift dialect starting sqlfluff 4.4.0 parses EXCLUDE
        # as a direct child of select_clause instead of part of
        # the wildcard, see https://github.com/sqlfluff/sqlfluff/pull/8270 for details
        if select_clause_element_tuple := wildcard_expression.get_parent():
            if select_clause_tuple := select_clause_element_tuple[0].get_parent():
                wildcard_except_segment = select_clause_tuple[0].get_children(
                    *wildcard_except_segment_types
                )
    columns = []
    for clause in wildcard_except_segment:
        container = clause.get_child("bracketed") or clause
        for sub_segment in container.get_children("identifier", "column_reference"):
            if sub_segment.type == "column_reference":
                identifiers = sub_segment.get_children("identifier")
                # a multi-part name like "col.field" (Databricks) excludes a struct
                # field, the column itself stays in the wildcard expansion
                if len(identifiers) == 1:
                    columns.append(identifiers[0].raw)
            else:
                columns.append(sub_segment.raw)
    return columns


def extract_innermost_bracketed(bracketed_segment: BaseSegment) -> BaseSegment:
    # in case of subquery in nested parenthesis like: `SELECT
    # * FROM ((table))`, find the innermost one first
    while True:
        sub_bracketed_segments = [
            bs.get_child("bracketed")
            for bs in bracketed_segment.segments
            if bs.get_child("bracketed")
        ]
        sub_paren = bracketed_segment.get_child("bracketed") or (
            sub_bracketed_segments[0] if sub_bracketed_segments else None
        )
        if sub_paren is not None:
            bracketed_segment = sub_paren
        else:
            break
    return bracketed_segment


def extract_source_columns(segment: BaseSegment) -> list[ColumnQualifierTuple]:
    """
    :param segment: segment to be processed
    :return: list of extracted source columns
    """
    col_list = []
    if segment.type in COLUMN_SEGMENT_TYPE or is_wildcard(segment):
        if cqt := extract_column_qualifier(segment):
            col_list = [cqt]
    elif segment.type in FUNCTION_SEGMENT_TYPE:
        for bracketed in segment.recursive_crawl("bracketed"):
            # the bracketed could be in function_contents
            # or over_clause in case of window function
            col_list += _extract_columns_from_parenthesis(bracketed)
    elif segment.type in NON_IDENTIFIER_OR_COLUMN_SEGMENT_TYPE:
        sub_segments = list_child_segments(segment)
        if segment.type in KEY_VALUE_SEGMENT_TYPE:
            sub_segments = sub_segments[1:]
        col_list = []
        for sub_segment in sub_segments:
            if sub_segment.type == "bracketed":
                if is_subquery(sub_segment):
                    col_list += _extract_columns_from_subquery(sub_segment)
                else:
                    col_list += _extract_columns_from_parenthesis(sub_segment)
            elif sub_segment.type in SOURCE_COLUMN_SEGMENT_TYPE or is_wildcard(
                sub_segment
            ):
                res = extract_source_columns(sub_segment)
                col_list.extend(res)
    return col_list


def _extract_columns_from_subquery(
    sub_segment: BaseSegment,
) -> list[ColumnQualifierTuple]:
    """
    :param sub_segment: segment to be processed
    :return: A list of source columns from a segment
    """
    # This is to avoid circular import
    from sqllineage.runner import LineageRunner

    src_cols = [
        lineage[0]
        for lineage in LineageRunner(
            sub_segment.raw, dialect=SQLPARSE_DIALECT
        ).get_column_lineage(exclude_path_ending_in_subquery=False)
    ]
    source_columns = [
        ColumnQualifierTuple(
            src_col.raw_name,
            # the full table name makes the outer query resolve this exact table,
            # not one with the same name in its own FROM clause
            str(src_col.parent) if src_col.parent else None,
        )
        for src_col in src_cols
        # columns of a subquery nested inside this one (e.g. literals) aren't
        # visible to the outer query
        if src_col.parent is None or isinstance(src_col.parent, Table)
    ]
    return source_columns


def _extract_columns_from_parenthesis(
    sub_segment: BaseSegment,
) -> list[ColumnQualifierTuple]:
    # windows function has an extra layer, get rid of
    # it so that it can be handled as regular functions
    if window_specification := sub_segment.get_child("window_specification"):
        sub_segment = window_specification
    col, _ = extract_column_and_alias(sub_segment, False)
    return col if col else []


def extract_column_and_alias(
    segment: BaseSegment, check_bracketed: bool = True
) -> tuple[list[ColumnQualifierTuple], str | None]:
    """
    check_bracketed is True for top-level column definition, like (col1 + col2) as col3
    set to False for bracket in function call, like coalesce(col1, col2) as col3
    """
    alias = None
    columns = []
    sub_segments = list_child_segments(segment, check_bracketed)
    for sub_segment in sub_segments:
        if sub_segment.type == "alias_expression":
            alias = extract_identifier(sub_segment)
        elif sub_segment.type in SOURCE_COLUMN_SEGMENT_TYPE or is_wildcard(sub_segment):
            res = extract_source_columns(sub_segment)
            columns += res if res else []
    return columns, alias
