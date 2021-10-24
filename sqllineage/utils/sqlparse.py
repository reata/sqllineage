from sqlparse import tokens as T
from sqlparse.sql import Function, Parenthesis, TokenList


def is_subquery(token: TokenList) -> bool:
    flag = False
    if isinstance(token, Parenthesis):
        _, sub_token = token.token_next_by(m=(T.DML, "SELECT"))
        if sub_token is not None:
            flag = True
    return flag


def get_parameters(token: Function):
    """
    This is a replacement for sqlparse.sql.Function.get_parameters(), which produces problematic result for:
        if(col1 = 'foo' AND col2 = 'bar', 1, 0)
    """
    return token.tokens[-1].get_sublists()
