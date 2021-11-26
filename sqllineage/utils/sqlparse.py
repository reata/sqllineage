from typing import List

from sqlparse import tokens as T
from sqlparse.sql import Case, Comparison, Function, Identifier, Parenthesis, TokenList

from sqllineage.utils.entities import SubQueryTuple


def is_subquery(token: TokenList) -> bool:
    flag = False
    if isinstance(token, Parenthesis):
        _, sub_token = token.token_next_by(m=(T.DML, "SELECT"))
        if sub_token is not None:
            flag = True
    return flag


def get_subquery_parentheses(token: Identifier) -> List[SubQueryTuple]:
    """
    Retrieve subquery list from identifier
    the returned list is either empty when no subquery parsed or list of [parenthesis, alias] tuple
    """
    subquery = []
    kw_idx, kw = token.token_next_by(m=(T.Keyword, "AS"))
    sublist = list(token.get_sublists())
    if kw is not None and len(sublist) == 1:
        # CTE: tbl AS (SELECT 1)
        target = sublist[0]
    else:
        # normal subquery: (SELECT 1) tbl
        target = token.token_first(skip_cm=True)
    if isinstance(target, Case):
        # CASE WHEN (SELECT count(*) from tab1) > 0 THEN (SELECT count(*) FROM tab1) ELSE -1
        for tk in target.get_sublists():
            if isinstance(tk, Comparison):
                if is_subquery(tk.left):
                    subquery.append(SubQueryTuple(tk.left, tk.left.get_real_name()))
                if is_subquery(tk.right):
                    subquery.append(SubQueryTuple(tk.right, tk.right.get_real_name()))
            elif is_subquery(tk):
                subquery.append(SubQueryTuple(tk, token.get_real_name()))
    if is_subquery(target):
        subquery = [SubQueryTuple(target, token.get_real_name())]
    return subquery


def get_parameters(token: Function):
    """
    This is a replacement for sqlparse.sql.Function.get_parameters(), which produces problematic result for:
        if(col1 = 'foo' AND col2 = 'bar', 1, 0)
    """
    return token.tokens[-1].get_sublists()
