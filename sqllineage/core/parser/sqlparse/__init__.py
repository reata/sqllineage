import re

from sqlparse import tokens
from sqlparse.engine import grouping
from sqlparse.keywords import KEYWORDS, SQL_REGEX

from sqllineage.core.parser.sqlparse.utils import group_function_with_window


def _patch_adding_window_function_token() -> None:
    grouping.group_functions = group_function_with_window


def _patch_adding_builtin_type() -> None:
    KEYWORDS["STRING"] = tokens.Name.Builtin
    KEYWORDS["DATETIME"] = tokens.Name.Builtin


def _patch_updating_lateral_view_lexeme() -> None:
    for i, (regex, lexeme) in enumerate(SQL_REGEX):
        rgx = re.compile(regex, re.IGNORECASE | re.UNICODE).match
        if rgx("LATERAL VIEW EXPLODE(col)"):
            new_regex = r"(LATERAL\s+VIEW\s+)(OUTER\s+)?(EXPLODE|INLINE|PARSE_URL_TUPLE|POSEXPLODE|STACK|JSON_TUPLE)\b"
            SQL_REGEX[i] = (new_regex, lexeme)
            break


def _monkey_patch() -> None:
    _patch_adding_window_function_token()
    _patch_adding_builtin_type()
    _patch_updating_lateral_view_lexeme()


_monkey_patch()
