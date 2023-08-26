"""
Utils class to deal with the sqlfluff segments manipulations

naming convention:
    is_x: BaseSegment -> bool
    find_x: BaseSegment -> Optional[BaseSegment]
    list_x: BaseSegment -> List[BaseSegment]
    extract_x: other pattern
first parameter of each function must be sqlfluff BaseSegment
"""
from typing import List, Optional, Tuple

from sqlfluff.core.parser import BaseSegment

from sqllineage.utils.entities import ColumnQualifierTuple, SubQueryTuple


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
    if segment.type == "from_expression_element" or segment.type == "bracketed":
        token = extract_innermost_bracketed(
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


def is_wildcard(segment: BaseSegment) -> bool:
    return segment.type == "wildcard_expression" or (
        segment.type == "symbol" and segment.raw == "*"
    )


def find_from_expression_element(segment: BaseSegment) -> Optional[BaseSegment]:
    """
    segment can be of type:
        from_clause as grandparent
        from_expression/join_clause as parent
    """
    from_expression_element = None
    if segment.type == "from_clause":
        if from_expression := segment.get_child("from_expression"):
            non_bracket = from_expression
            while non_bracket.get_child("bracketed"):
                non_bracket = non_bracket.get_child("bracketed")
            if seg := non_bracket.get_child("from_expression_element"):
                from_expression_element = seg
            elif seg := non_bracket.get_child("from_expression"):
                if sub_seg := seg.get_child("from_expression_element"):
                    from_expression_element = sub_seg
    elif segment.type in ("from_expression", "join_clause"):
        if seg := segment.get_child("from_expression_element"):
            from_expression_element = seg
    return from_expression_element


def find_table_identifier(segment: BaseSegment) -> Optional[BaseSegment]:
    """
    recursively find table identifier
    """
    table_identifier = None
    if segment.type in ["table_reference", "file_reference", "object_reference"]:
        return segment
    else:
        for sub_segment in segment.segments:
            if identifier := find_table_identifier(sub_segment):
                return identifier
    return table_identifier


def list_join_clause(segment: BaseSegment) -> List[BaseSegment]:
    """
    traverse from_clause, recursively goes into bracket by default
    """
    if from_expression := segment.get_child("from_expression"):
        if bracketed := from_expression.get_child("bracketed"):
            join_clauses = bracketed.get_children("join_clause")
            if inner_bracket := bracketed.get_child("bracketed"):
                join_clauses = list_join_clause(inner_bracket) + join_clauses
            return join_clauses
        else:
            return from_expression.get_children("join_clause")
    return []


def list_expression_from_when_clause(
    when_clause: BaseSegment, sub_type: str
) -> List[BaseSegment]:
    """
    sub_type can be either WHEN or THEN, this is to extract subquery for case when (SELECT ...) then (SELECT ...)
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


def list_subqueries(segment: BaseSegment) -> List[SubQueryTuple]:
    subquery = []
    if segment.type == "select_clause":
        as_segment = segment.get_child("select_clause_element").get_child(
            "alias_expression"
        )
        select_clause = segment.get_child("select_clause_element")
        case_expression = select_clause.get_child(
            "expression"
        ) and select_clause.get_child("expression").get_child("case_expression")
        target = case_expression or select_clause.get_child("column_reference")
        if target and target.type == "case_expression":
            for when_clause in target.get_children("when_clause"):
                for bracketed_segment in list_expression_from_when_clause(
                    when_clause, sub_type="WHEN"
                ):
                    subquery.append(SubQueryTuple(bracketed_segment, None))
                for bracketed_segment in list_expression_from_when_clause(
                    when_clause, sub_type="THEN"
                ):
                    subquery.append(
                        SubQueryTuple(bracketed_segment, extract_identifier(as_segment))
                    )
    elif segment.type == "from_expression_element":
        as_segment, target = extract_as_and_target_segment(segment)
        if is_subquery(target):
            as_segment, target = extract_as_and_target_segment(segment)
            subquery = [
                SubQueryTuple(
                    extract_innermost_bracketed(target)
                    if not is_set_expression(target)
                    else target,
                    extract_identifier(as_segment) if as_segment else None,
                )
            ]
    elif segment.type == "where_clause":
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
    elif segment.type in ["from_clause", "from_expression"]:
        if from_expression_element := find_from_expression_element(segment):
            subquery = list_subqueries(from_expression_element)
        for join_clause in list_join_clause(segment):
            if from_expression_element := find_from_expression_element(join_clause):
                subquery += list_subqueries(from_expression_element)
    elif is_set_expression(segment):
        subquery = [
            SubQueryTuple(s, None)
            for s in list_child_segments(segment, check_bracketed=True)
            if s.type in ("bracketed", "select_statement")
        ]
    return subquery


def list_child_segments(
    segment: BaseSegment, check_bracketed: bool = True
) -> List[BaseSegment]:
    """
    Filter segments for a given segment's children, recursive goes into bracket by default
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
    col_identifier = identifiers[-1]
    return str(col_identifier.raw)


def extract_as_and_target_segment(
    segment: BaseSegment,
) -> Tuple[BaseSegment, BaseSegment]:
    as_segment = segment.get_child("alias_expression")
    sublist = list_child_segments(segment, False)
    target = sublist[0] if is_subquery(sublist[0]) else sublist[0].segments[0]
    return as_segment, target


def extract_column_qualifier(segment: BaseSegment) -> Optional[ColumnQualifierTuple]:
    cqt = None
    if is_wildcard(segment):
        identifiers = segment.raw.split(".")
        column = identifiers[-1]
        parent = identifiers[-2] if len(identifiers) > 1 else None
        cqt = ColumnQualifierTuple(column, parent)
    elif segment.type == "column_reference":
        sub_segments = list_child_segments(segment)
        column = sub_segments[-1].raw
        parent = sub_segments[-2].raw if len(sub_segments) > 1 else None
        cqt = ColumnQualifierTuple(column, parent)
    elif segment.type == "identifier":
        cqt = ColumnQualifierTuple(segment.raw, None)
    return cqt


def extract_innermost_bracketed(bracketed_segment: BaseSegment) -> BaseSegment:
    # in case of subquery in nested parenthesis like: `SELECT * FROM ((table))`, find the innermost one first
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


def get_child(segment: BaseSegment, *seg_type: str) -> BaseSegment:
    """
    bypass mypy type check as sqlfluff get_child is untyped yet
    """
    return segment.get_child(*seg_type)


def get_children(segment: BaseSegment, *seg_type: str) -> List[BaseSegment]:
    """
    bypass mypy type check as sqlfluff get_children is untyped yet
    """
    return segment.get_children(*seg_type)
