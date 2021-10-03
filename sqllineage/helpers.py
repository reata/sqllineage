import logging
from argparse import Namespace
from enum import Enum, unique


logger = logging.getLogger(__name__)


class NodeTag:
    READ = "read"
    WRITE = "write"
    CTE = "cte"
    DROP = "drop"
    SOURCE_ONLY = "source_only"
    TARGET_ONLY = "target_only"
    SELFLOOP = "selfloop"


@unique
class EdgeType(Enum):
    LINEAGE = 1
    HAS_COLUMN = 2
    RENAME = 3


class LineageLevel:
    TABLE = "table"
    COLUMN = "column"


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
            logger.exception("Permission denied when reading file '%s'", args.f)
            exit(1)
    elif getattr(args, "e", None):
        sql = args.e
    return sql
