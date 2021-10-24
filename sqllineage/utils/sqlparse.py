from sqlparse import tokens as T
from sqlparse.sql import Parenthesis, TokenList


def is_subquery(token: TokenList) -> bool:
    flag = False
    if isinstance(token, Parenthesis):
        _, sub_token = token.token_next_by(m=(T.DML, "SELECT"))
        if sub_token is not None:
            flag = True
    return flag
