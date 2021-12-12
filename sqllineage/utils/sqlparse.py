import itertools
from typing import Iterator, List

from sqlparse.engine.grouping import _group, group_functions
from sqlparse.sql import Case, Comparison, Function, Identifier, Parenthesis, TokenList
from sqlparse.tokens import DML, Keyword, Wildcard
from sqlparse.utils import recurse

from sqllineage.utils.entities import SubQueryTuple


def is_subquery(token: TokenList) -> bool:
    flag = False
    if isinstance(token, Parenthesis):
        _, sub_token = token.token_next_by(m=(DML, "SELECT"))
        if sub_token is not None:
            flag = True
    return flag


def get_subquery_parentheses(token: Identifier) -> List[SubQueryTuple]:
    """
    Retrieve subquery list from identifier
    the returned list is either empty when no subquery parsed or list of [parenthesis, alias] tuple
    """
    subquery = []
    as_idx, as_ = token.token_next_by(m=(Keyword, "AS"))
    sublist = list(token.get_sublists())
    if as_ is not None and len(sublist) == 1:
        # CTE: tbl AS (SELECT 1)
        target = sublist[0]
    else:
        if isinstance(token, Function):
            # CTE without AS: tbl (SELECT 1)
            target = token.tokens[-1]
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
    This implementation ignores the constant parameter as we don't need them for column lineage for now
    """
    if isinstance(token, Window):
        return token.get_parameters()
    else:
        return [
            tk for tk in token.tokens[-1].tokens if tk.is_group or tk.ttype == Wildcard
        ]


class Window(Function):
    """window function + OVER keyword + window defn"""

    def get_parameters(self) -> Iterator[TokenList]:
        return itertools.chain(
            get_parameters(self.get_window_function()),
            self.get_window_defn().get_sublists(),
        )

    def get_window_function(self) -> Function:
        return self.tokens[0]

    def get_window_defn(self) -> Parenthesis:
        return self.tokens[-1]


@recurse(Window)
def group_window(tlist):
    def match(token):
        return token.is_keyword and token.normalized == "OVER"

    def valid_prev(token):
        return isinstance(token, Function)

    def valid_next(token):
        return isinstance(token, Parenthesis)

    def post(tlist, pidx, tidx, nidx):
        return pidx, nidx

    _group(
        tlist, Window, match, valid_prev, valid_next, post, extend=False, recurse=False
    )


def group_function_with_window(tlist):
    group_functions(tlist)
    group_window(tlist)
