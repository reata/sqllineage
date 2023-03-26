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
        if regex("LATERAL VIEW EXPLODE(col)"):
            new_regex = r"(LATERAL\s+VIEW\s+)(OUTER\s+)?(EXPLODE|INLINE|PARSE_URL_TUPLE|POSEXPLODE|STACK|JSON_TUPLE)\b"
            new_compile = re.compile(new_regex, re.IGNORECASE | re.UNICODE).match
            SQL_REGEX[i] = (new_compile, lexeme)
            break


def _monkey_patch() -> None:
    _patch_adding_window_function_token()
    _patch_adding_builtin_type()
    _patch_updating_lateral_view_lexeme()


_monkey_patch()
