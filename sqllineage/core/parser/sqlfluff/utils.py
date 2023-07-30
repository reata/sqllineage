"""
Utils class to deal with the sqlfluff segments manipulations
"""
import re
from typing import Any, List, Optional, Tuple

from sqlfluff.core.linter import ParsedString
from sqlfluff.core.parser import BaseSegment

from sqllineage.utils.entities import SubQueryTuple


def is_segment_negligible(segment: BaseSegment) -> bool:
    """
    :param segment: segment to be processed segment to be processed
    :return: True is the segment is negligible
    """
    return (
        segment.is_whitespace
        or segment.is_comment
        or bool(segment.is_meta)
        or (segment.type == "symbol" and segment.raw != "*")
    )


def extract_as_and_target_segment(
    segment: BaseSegment,
) -> Tuple[BaseSegment, BaseSegment]:
    """
    :param segment: segment to be processed
    :return: a Tuple of "alias_expression" and the target table
    """
    as_segment = segment.get_child("alias_expression")
    sublist = list_child_segments(segment, False)
    target = sublist[0] if is_subquery(sublist[0]) else sublist[0].segments[0]
    return as_segment, target


def get_subqueries(segment: BaseSegment) -> List[SubQueryTuple]:
    """
    Retrieve a list of 'SubQueryTuple' based on the type of the segment.
    :param segment: segment to be processed
    :return: a list of 'SubQueryTuple'
    """
    subquery = []
    if segment.type in ["select_clause"]:
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
                for bracketed_segment in get_bracketed_from(
                    when_clause, to_keyword="THEN", children_segments="expression"
                ):
                    subquery.append(SubQueryTuple(bracketed_segment, None))
                for bracketed_segment in get_bracketed_from(
                    when_clause, from_keyword="THEN", children_segments="expression"
                ):
                    subquery.append(
                        SubQueryTuple(bracketed_segment, get_identifier(as_segment))
                    )
        return subquery
    elif segment.type in ["from_clause", "from_expression", "from_expression_element"]:
        if from_expression_element := get_from_expression_element(segment):
            as_segment, target = extract_as_and_target_segment(from_expression_element)
            if is_subquery(target):
                as_segment, target = extract_as_and_target_segment(
                    from_expression_element
                )
                subquery = [
                    SubQueryTuple(
                        get_innermost_bracketed(target)
                        if not is_union(target)
                        else target,
                        get_identifier(as_segment) if as_segment else None,
                    )
                ]
        return subquery
    elif segment.type in ["where_clause"]:
        expression_segments = segment.get_child("expression").segments or []
        bracketed_segments = [s for s in expression_segments if s.type == "bracketed"]
        if bracketed_segments and is_subquery(bracketed_segments[0]):
            return [SubQueryTuple(get_innermost_bracketed(bracketed_segments[0]), None)]
        return []
    elif is_union(segment):
        for s in list_child_segments(segment, check_bracketed=True):
            if s.type == "bracketed" or s.type == "select_statement":
                subquery.append(SubQueryTuple(s, None))
        return subquery
    else:
        raise NotImplementedError()


def is_subquery(segment: BaseSegment) -> bool:
    """
    :param segment: segment to be processed
    :return: True if the given segment is subquery
    """
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


def get_innermost_bracketed(bracketed_segment: BaseSegment) -> BaseSegment:
    """
    :param bracketed_segment: segment to be processed
    :return:the innermost bracketed segment of a bracketed segment
    """
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
    """
    :param segment: segment to be processed
    :return: True if it is a given segments list o segment contains a 'values_clause' type segment
    """
    if segment.get_child("table_expression") and segment.get_child(
        "table_expression"
    ).get_child("values_clause"):
        return True
    return False


def list_from_expression(segment: BaseSegment) -> List[BaseSegment]:
    if segment.type == "from_clause":
        return [seg for seg in segment.get_children("from_expression")]
    return []


def get_from_expression_element(segment: BaseSegment) -> Optional[BaseSegment]:
    """
    capable of handing:
        from_clause as grandparent
        from_expression/join_clause as parent
        from_expression_element itself
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
    elif segment.type == "from_expression_element":
        from_expression_element = segment
    return from_expression_element


def filter_segments_by_keyword(
    statement: BaseSegment,
    from_keyword: Optional[str] = None,
    to_keyword: Optional[str] = None,
    children_segments: Optional[str] = None,
) -> List[BaseSegment]:
    """
    Filter from the segments' children of a segment by expected children segment
    :param statement: segment to be processed
    :param from_keyword: start in a specific keyword from the segments list
    :param to_keyword: end at a specific keyword from the segments list
    :param children_segments: the filtered children segment, otherwise all of them
    :return: a list of segments' children of a segment if children_segments is not None otherwise all of them
    """
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
    from_keyword: Optional[str] = None,
    to_keyword: Optional[str] = None,
    children_segments: Optional[str] = None,
) -> List[BaseSegment]:
    """
    Filter from the segments' children of a segment by expected children segment
    :param segment: segment to be processed
    :param from_keyword: start in a specific keyword from the segments list
    :param to_keyword: end at a specific keyword from the segments list
    :param children_segments: the filtered children segment, otherwise all of them
    :return: a list of segments' children of a segment which type is "bracketed"
    """
    return [
        bracketed_segment
        for filtered_segment in filter_segments_by_keyword(
            segment, from_keyword, to_keyword, children_segments
        )
        for bracketed_segment in filtered_segment.get_children("bracketed")
    ]


def find_table_identifier(identifier: BaseSegment) -> Optional[BaseSegment]:
    """
    :param identifier: segment to be processed
    :return: a table_reference or file_reference type segment if it exists in children list, otherwise the identifier
    """
    table_identifier = None
    if identifier.segments:
        for segment in identifier.segments:
            if segment.type in ("table_reference", "file_reference"):
                return segment
            else:
                table_identifier = find_table_identifier(segment)
                if table_identifier:
                    return table_identifier
    return table_identifier


def list_child_segments(
    segment: BaseSegment, check_bracketed: bool = True
) -> List[BaseSegment]:
    """
    Filter segments for a given segment's children, recursive goes into bracket by default
    """
    if segment.type == "bracketed" and check_bracketed:
        if is_union(segment):
            return [seg for seg in segment.segments if seg.type == "set_expression"]
        else:
            result = []
            for seg in segment.iter_segments(
                expanding=["expression"], pass_through=True
            ):
                if seg.type == "column_reference":
                    result.append(seg)
                else:
                    for s in seg.segments:
                        if not is_segment_negligible(s):
                            result.append(s)
            return result
    else:
        return [seg for seg in segment.segments if not is_segment_negligible(seg)]


def get_identifier(col_segment: BaseSegment) -> str:
    """
    :param col_segment: column segment
    :return: the table identifier name
    """
    identifiers = list_child_segments(col_segment)
    col_identifier = identifiers[-1]
    return str(col_identifier.raw)


def is_wildcard(symbol: BaseSegment):
    """
    :param symbol: symbol segment
    :return: True if the symbol segment is a wildcard
    """
    return (
        symbol.type == "wildcard_expression"
        or symbol.type == "symbol"
        and symbol.raw == "*"
    )


def get_table_alias(table_segments: List[BaseSegment]) -> Optional[str]:
    """
    :param table_segments: list of segments to search for an alias
    :return: alias found otherwise None
    """
    alias = None
    if len(table_segments) > 1 and table_segments[1].type == "alias_expression":
        segments = list_child_segments(table_segments[1])
        alias = segments[1].raw if len(segments) > 1 else segments[0].raw
    return str(alias) if alias else None


def has_alias(segment: BaseSegment) -> bool:
    """
    :param segment: segment to be processed
    :return: True if the segment contains an alias keyword
    """
    return len([s for s in segment.get_children("keyword") if s.raw_upper == "AS"]) > 0


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


def get_grandchild(
    segment: BaseSegment, child: str, grandchild: str
) -> Optional[BaseSegment]:
    """
    :param segment: segment to be processed
    :param child: child segment
    :param grandchild: grand child
    :return: the grandchild segment if found, otherwise, None
    """
    return (
        segment.get_child(child).get_child(grandchild)
        if segment.get_child(child)
        else None
    )


def get_child(segment: BaseSegment, child: str) -> BaseSegment:
    """
    :param segment: segment to be processed
    :param child: child segment
    :return:
    """
    return segment.get_child(child)


def get_grandchildren(segment: BaseSegment, child: str, grandchildren: str) -> Any:
    """
    :param segment: segment to be processed
    :param child: child segment
    :param grandchildren: grand children
    :return: the grandchildren segment list if found, otherwise, empty list
    """
    return (
        segment.get_child(child).get_children(grandchildren)
        if segment.get_child(child)
        else []
    )


def get_statement_segment(parsed_string: ParsedString) -> BaseSegment:
    """
    :param parsed_string: parsed string
    :return: first segment from the statement segment of the segments of parsed_string
    """
    return next(
        (
            x.segments[0]
            if x.type == "statement"
            else x.get_child("statement").segments[0]
            for x in getattr(parsed_string.tree, "segments")
            if x.type == "statement" or x.type == "batch"
        )
    )


def is_union(segment: BaseSegment) -> bool:
    return segment.type == "set_expression" or any(
        seg.type == "set_expression" for seg in segment.segments
    )


def is_subquery_statement(stmt: str) -> bool:
    parentheses_regex = r"^\(.*\)"
    return bool(re.match(parentheses_regex, stmt))


def remove_statement_parentheses(stmt: str) -> str:
    parentheses_regex = r"^\((.*)\)"
    return re.sub(parentheses_regex, r"\1", stmt)


def clean_parentheses(stmt: str) -> str:
    """
      Clean redundant parentheses from a SQL statement e.g:
        `SELECT col1 FROM (((((((SELECT col1 FROM tab1))))))) dt`
      will be:
        `SELECT col1 FROM (SELECT col1 FROM tab1) dt`

    :param stmt: a SQL str to be cleaned
    """
    redundant_parentheses = r"\(\(([^()]+)\)\)"
    if re.findall(redundant_parentheses, stmt):
        stmt = re.sub(redundant_parentheses, r"(\1)", stmt)
        stmt = clean_parentheses(stmt)
    return stmt
