from typing import List, Any, Optional, Tuple

from sqlfluff.core.parser import BaseSegment
from sqlfluff.core.parser.segments import BracketedSegment

from sqllineage.sqlfluff_core.utils.entities import SubSqlFluffQueryTuple


def is_segment_negligible(segment: BaseSegment) -> bool:
    # utility to skip tokens like whitespace, comment, meta or symbol
    return (
        segment.is_whitespace
        or segment.is_comment
        or bool(segment.is_meta)
        or (segment.type == "symbol" and segment.raw != "*")
    )


def get_bracketed_sub_queries_select(
    segment: BaseSegment,
) -> List[SubSqlFluffQueryTuple]:
    """
    Retrieve subquery list
    the returned list is either empty when no subquery parsed or list of [bracketed_segment, alias] tuple
    """
    subquery = []
    as_segment = segment.get_child("select_clause_element").get_child(
        "alias_expression"
    )
    select_clause = segment.get_child("select_clause_element")
    sublist = list(
        [
            seg
            for seg in select_clause.segments
            if not is_segment_negligible(seg) and seg.type != "table_expression"
        ]
    )
    if as_segment is not None and len(sublist) == 1:
        # CTE: tbl AS (SELECT 1)
        target = sublist[0]
    else:
        case_expression = select_clause.get_child(
            "expression"
        ) and select_clause.get_child("expression").get_child("case_expression")
        target = case_expression or select_clause.get_child("column_reference")
    if target and target.type == "case_expression":
        for when_clause in target.get_children("when_clause"):
            for bracketed_segment in get_bracketed_from(
                when_clause, to_keyword="THEN", children_segments="expression"
            ):
                subquery.append(SubSqlFluffQueryTuple(bracketed_segment, None))
            for bracketed_segment in get_bracketed_from(
                when_clause, from_keyword="THEN", children_segments="expression"
            ):
                subquery.append(
                    SubSqlFluffQueryTuple(bracketed_segment, get_identifier(as_segment))
                )
        for else_clause in target.get_children("else_clause"):
            for bracketed_segment in get_bracketed_from(
                else_clause, children_segments="expression"
            ):
                subquery.append(
                    SubSqlFluffQueryTuple(bracketed_segment, get_identifier(as_segment))
                )
    if target and is_subquery(target):
        subquery = [
            SubSqlFluffQueryTuple(get_innermost_bracketed(target), as_segment.raw)
        ]
    return subquery


def get_bracketed_sub_queries_from(
    segment: BaseSegment, skip_union: bool = True
) -> List[SubSqlFluffQueryTuple]:
    subquery = []
    as_segment, target = extract_as_and_target_segment(
        get_inner_from_expression(segment)
    )
    if not skip_union and is_union(target):
        for sq in get_union_sub_queries(target):
            subquery.append(
                SubSqlFluffQueryTuple(
                    sq,
                    get_identifier(as_segment) if as_segment else None,
                )
            )
    elif is_subquery(target):
        as_segment, target = extract_as_and_target_segment(
            get_inner_from_expression(segment)
        )
        subquery = [
            SubSqlFluffQueryTuple(
                get_innermost_bracketed(target),
                get_identifier(as_segment) if as_segment else None,
            )
        ]
    return subquery


def extract_as_and_target_segment(
    segment: BaseSegment,
) -> Tuple[BaseSegment, BaseSegment]:
    as_segment = segment.get_child("alias_expression")
    sublist = list([seg for seg in segment.segments if not is_segment_negligible(seg)])
    if as_segment is not None and len(sublist) == 1:
        target = sublist[0]
    else:
        target = sublist[0] if is_subquery(sublist[0]) else sublist[0].segments[0]
    return as_segment, target


def get_bracketed_sub_queries_where(
    segment: BaseSegment,
) -> List[SubSqlFluffQueryTuple]:
    expression_segments = segment.get_child("expression").segments or []
    bracketed_segments = [seg for seg in expression_segments if seg.type == "bracketed"]
    if bracketed_segments and is_subquery(bracketed_segments[0]):
        return [
            SubSqlFluffQueryTuple(get_innermost_bracketed(bracketed_segments[0]), None)
        ]
    return []


def get_sub_queries(
    segment: BaseSegment, skip_union: bool = True
) -> List[SubSqlFluffQueryTuple]:
    if segment.type in ["select_clause"]:
        return get_bracketed_sub_queries_select(segment)
    elif segment.type in ["from_clause", "from_expression", "from_expression_element"]:
        return get_bracketed_sub_queries_from(segment, skip_union)
    elif segment.type in ["join_clause"]:
        return []
    elif segment.type in ["where_clause"]:
        return get_bracketed_sub_queries_where(segment)
    else:
        raise NotImplementedError()


def is_subquery(segment: BaseSegment) -> bool:
    if segment.type == "from_expression_element" or segment.type == "bracketed":
        token = get_innermost_bracketed(
            segment if segment.type == "bracketed" else segment.segments[0]
        )
        # check if innermost parenthesis contains SELECT
        sub_token = (
            token.get_child("select_statement")
            or token.get_child("set_expression")
            or (
                token.get_child("expression")
                and token.get_child("expression").get_child("select_statement")
            )
        )
        if sub_token is not None:
            return True
    return False


def get_innermost_bracketed(bracketed_segment: BaseSegment) -> BracketedSegment:
    # in case of subquery in nested parenthesis, find the innermost one first
    # this should not occur if we clean parentheses like: `SELECT * FROM ((table))`
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


def is_values_clause(segment: BaseSegment) -> bool:
    if segment.get_child("table_expression") and segment.get_child(
        "table_expression"
    ).get_child("values_clause"):
        return True
    for s in segment.segments:
        if is_segment_negligible(s):
            continue
        if s.type == "values_clause":
            return True
    return False


def get_multiple_identifiers(segment: BaseSegment) -> List[Any]:
    if segment.type == "from_clause":
        return [seg for seg in segment.get_children("from_expression")]
    return []


def get_inner_from_expression(segment: BaseSegment) -> BaseSegment:
    if segment.get_child("from_expression") and segment.get_child(
        "from_expression"
    ).get_child("from_expression_element"):
        return segment.get_child("from_expression").get_child("from_expression_element")
    elif segment.get_child("from_expression_element"):
        return segment.get_child("from_expression_element")
    else:
        return segment


def filter_segments_by_keyword(
    statement: BaseSegment,
    from_keyword: str = None,
    to_keyword: str = None,
    children_segments: str = None,
) -> List[BaseSegment]:
    filtered_segments = []
    can_append = False if from_keyword else True
    for segment in statement.segments:
        if not can_append and segment.type == "keyword" and from_keyword:
            if segment.raw.lower() == from_keyword.lower():
                can_append = True
        if can_append:
            filtered_segments.append(segment)
        if segment.type == "keyword" and to_keyword:
            if segment.raw.lower() == to_keyword.lower():
                break
    return [
        segment
        for segment in filtered_segments
        if (children_segments is None or segment.type == children_segments)
    ]


def get_bracketed_from(
    segment: BaseSegment,
    from_keyword: str = None,
    to_keyword: str = None,
    children_segments: str = None,
) -> List[BracketedSegment]:
    return [
        bracketed_segment
        for filtered_segment in filter_segments_by_keyword(
            segment, from_keyword, to_keyword, children_segments
        )
        for bracketed_segment in filtered_segment.get_children("bracketed")
    ]


def find_table_identifier(identifier: BaseSegment) -> BaseSegment:
    table_identifier = None
    if identifier.segments:
        for segment in identifier.segments:
            if segment.type == "table_reference":
                return segment
            else:
                table_identifier = find_table_identifier(segment)
                if table_identifier:
                    return table_identifier
    return table_identifier


def retrieve_segments(
    statement: BaseSegment, check_bracketed: bool = True
) -> List[BaseSegment]:
    if statement.type == "bracketed" and check_bracketed:
        segments = [
            segment
            for segment in statement.iter_segments(
                expanding=["expression"], pass_through=True
            )
        ]
        result = []
        for segment in segments:
            if segment.type == "column_reference":
                result.append(segment)
            else:
                for seg in segment.segments:
                    if not is_segment_negligible(seg):
                        result.append(seg)
        return result
    else:
        return [seg for seg in statement.segments if not is_segment_negligible(seg)]


def get_identifier(col_segment: BaseSegment) -> str:
    identifiers = retrieve_segments(col_segment)
    col_identifier = identifiers[-1]
    return str(col_identifier.raw)


def is_wildcard(symbol: BaseSegment):
    return (
        symbol.type == "wildcard_expression"
        or symbol.type == "symbol"
        and symbol.raw == "*"
    )


def get_table_alias(table_tokes: List[BaseSegment]) -> Optional[str]:
    alias = None
    if len(table_tokes) > 1 and table_tokes[1].type == "alias_expression":
        alias = retrieve_segments(table_tokes[1])
        alias = alias[1].raw if len(alias) > 1 else alias[0].raw
    elif len(table_tokes) == 1 and table_tokes[0].type == "from_expression_element":
        table_and_alias = retrieve_segments(table_tokes[0])
        if len(table_and_alias) > 1 and table_and_alias[1].type == "alias_expression":
            alias = retrieve_segments(table_and_alias[1])
            alias = alias[1].raw if len(alias) > 1 else alias[0].raw
    return str(alias) if alias else None


def has_alias(segment: BaseSegment) -> bool:
    return (
        len([s for s in segment.get_children("keyword") if s.raw.lower() == "as"]) > 0
    )


def is_union(segment: BaseSegment) -> bool:
    sub_segments = retrieve_segments(segment, check_bracketed=True)
    return (
        len(
            [
                s
                for s in sub_segments
                if s.type == "set_operator"
                and (s.raw.lower() == "union" or s.raw.lower() == "union all")
            ]
        )
        > 0
    )


def get_union_sub_queries(segment: BaseSegment) -> List[BaseSegment]:
    sub_segments = retrieve_segments(segment, check_bracketed=True)
    return [
        s for s in sub_segments if s.type == "bracketed" or s.type == "select_statement"
    ]
