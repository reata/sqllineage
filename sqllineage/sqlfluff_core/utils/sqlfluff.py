from typing import List, Any, Optional

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
        for bracketed_segment in get_bracketed_from_case(target):
            subquery.append(SubSqlFluffQueryTuple(bracketed_segment, None))
    if target and is_subquery(target):
        subquery = [
            SubSqlFluffQueryTuple(get_innermost_bracketed(target), as_segment.raw)
        ]
    return subquery


def get_bracketed_sub_queries_from(segment: BaseSegment) -> List[SubSqlFluffQueryTuple]:
    subquery = []
    as_segment = segment.get_child("alias_expression")
    sublist = list([seg for seg in segment.segments if not is_segment_negligible(seg)])
    if as_segment is not None and len(sublist) == 1:
        # CTE: tbl AS (SELECT 1)
        target = sublist[0]
    else:
        target = sublist[0] if is_subquery(sublist[0]) else sublist[0].segments[0]
    if is_subquery(target):
        subquery = [
            SubSqlFluffQueryTuple(
                get_innermost_bracketed(target), as_segment.raw if as_segment else None
            )
        ]
    return subquery


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


def get_sub_queries(segment: BaseSegment) -> List[SubSqlFluffQueryTuple]:
    if segment.type in ["select_clause"]:
        return get_bracketed_sub_queries_select(segment)
    elif segment.type in ["from_clause", "from_expression", "from_expression_element"]:
        return get_bracketed_sub_queries_from(get_inner_from_expression(segment))
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


def get_bracketed_from_case(segment: BaseSegment) -> List[BracketedSegment]:
    expressions = [
        segment.get_children("expression")
        for segment in segment.get_children("when_clause", "else_clause")
    ]
    bracketed_list = [
        expr.get_children("bracketed")
        for expression in expressions
        for expr in expression
    ]
    return [
        bracketed
        for bracketed_sublist in bracketed_list
        for bracketed in bracketed_sublist
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
        return [
            seg
            for segment in segments
            for seg in segment.segments
            if not is_segment_negligible(seg)
        ]
    else:
        return [seg for seg in statement.segments if not is_segment_negligible(seg)]


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
    return alias
