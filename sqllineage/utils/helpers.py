import logging
import re
from argparse import Namespace

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


def is_subquery_statement(stmt: str) -> bool:
    parentheses_regex = r"^\(.*\)"
    return bool(re.match(parentheses_regex, stmt))


def remove_statement_parentheses(stmt: str) -> str:
    parentheses_regex = r"^\((.*)\)"
    return re.sub(parentheses_regex, r"\1", stmt)
