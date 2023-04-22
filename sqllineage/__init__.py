import os


def _patch_adding_window_function_token() -> None:
    from sqlparse.engine import grouping
    from sqllineage.utils.sqlparse import group_function_with_window

    grouping.group_functions = group_function_with_window


def _patch_adding_builtin_type() -> None:
    from sqlparse import tokens
    from sqlparse.keywords import KEYWORDS

    KEYWORDS["STRING"] = tokens.Name.Builtin
    KEYWORDS["DATETIME"] = tokens.Name.Builtin


def _patch_updating_lateral_view_lexeme() -> None:
    import re
    from sqlparse.keywords import SQL_REGEX

    for i, (regex, lexeme) in enumerate(SQL_REGEX):
        rgx = re.compile(regex, re.IGNORECASE | re.UNICODE).match
        if rgx("LATERAL VIEW EXPLODE(col)"):
            new_regex = r"(LATERAL\s+VIEW\s+)(OUTER\s+)?(EXPLODE|INLINE|PARSE_URL_TUPLE|POSEXPLODE|STACK|JSON_TUPLE)\b"
            SQL_REGEX[i] = (new_regex, lexeme)
            break


def _monkey_patch() -> None:
    try:
        _patch_adding_window_function_token()
        _patch_adding_builtin_type()
        _patch_updating_lateral_view_lexeme()
    except ImportError:
        # when imported by setup.py for constant variables, dependency is not ready yet
        pass


_monkey_patch()

NAME = "sqllineage"
VERSION = "1.3.8"
DEFAULT_LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {"default": {"format": "%(levelname)s: %(message)s"}},
    "handlers": {
        "console": {
            "level": "WARNING",
            "class": "logging.StreamHandler",
            "formatter": "default",
        }
    },
    "loggers": {
        "": {
            "handlers": ["console"],
            "level": "WARNING",
            "propagate": False,
            "filters": [],
        },
        "werkzeug": {
            "handlers": ["console"],
            "level": "ERROR",
            "propagate": False,
            "filters": [],
        },
    },
}

STATIC_FOLDER = "build"
DATA_FOLDER = os.environ.get(
    "SQLLINEAGE_DIRECTORY", os.path.join(os.path.dirname(__file__), "data")
)
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 5000
