import logging
from argparse import Namespace

logger = logging.getLogger(__name__)


def escape_identifier_name(name: str):
    return name.strip("`").strip('"')


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
            logger.exception("Permission denied when reading file '%s'", args.f)
            exit(1)
    elif getattr(args, "e", None):
        sql = args.e
    return sql
