import logging
from argparse import Namespace

logger = logging.getLogger(__name__)


def escape_identifier_name(name: str):
    """
    conform to ANSI SQL standard that:
        1) unquoted identifier name is case-insensitive, convert to lower case
        2) quoted identifier name is case-sensitive, reserve case and remove quote char
    Reference: https://stackoverflow.com/a/19933159
    """
    quote_chars = ["`", '"', "'"]
    if name.startswith("[") and name.endswith("]"):
        # tsql allows quoted identifier with square brackets, see reference
        # https://learn.microsoft.com/en-us/sql/relational-databases/databases/database-identifiers?view=sql-server-ver16#classes-of-identifiers
        # check brackets first so a bracketed identifier containing a quote char
        # (e.g. [O'Brien]) is still correctly unquoted
        return name.strip("[]")
    elif any(quote_char in name for quote_char in quote_chars):
        for quote_char in quote_chars:
            name = name.strip(quote_char)
        return name
    else:
        return name.lower()


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


def extract_file_path_from_args(args: Namespace) -> str:
    file_path = "."
    if getattr(args, "f", None):
        file_path = args.f
    return file_path
