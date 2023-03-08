import logging
from argparse import Namespace
from typing import List

logger = logging.getLogger(__name__)


def escape_identifier_name(name: str):
    return name.strip("`").strip('"').strip("'")


def extract_sql_from_args(args: Namespace) -> str:
    sql = ""
    if getattr(args, "f", None):
        try:
            with open(args.f) as f:
                sql = f.read()
        except IsADirectoryError:
            logger.exception("%s is a directory", args.f)
            exit(1)
        except FileNotFoundError:
            logger.exception("No such file: %s", args.f)
            exit(1)
        except PermissionError:
            # On Windows, open a directory as file throws PermissionError
            logger.exception("Permission denied when reading file '%s'", args.f)
            exit(1)
    elif getattr(args, "e", None):
        sql = args.e
    return sql


def split(sql: str) -> List[str]:
    # TODO: we need a parser independent split function
    import sqlparse

    # sometimes sqlparse split out a statement that is comment only, we want to exclude that
    return [s.value for s in sqlparse.parse(sql) if s.token_first(skip_cm=True)]


def trim_comment(sql: str) -> str:
    # TODO: we need a parser independent trim_comment function
    import sqlparse

    return str(sqlparse.format(sql, strip_comments=True))
