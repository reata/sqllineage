from enum import StrEnum


class NodeTag(StrEnum):
    READ = "read"
    WRITE = "write"
    CTE = "cte"
    DROP = "drop"
    SOURCE_ONLY = "source_only"
    TARGET_ONLY = "target_only"
    SELFLOOP = "selfloop"


class EdgeTag(StrEnum):
    INDEX = "index"


class EdgeType(StrEnum):
    LINEAGE = "lineage"
    RENAME = "rename"
    HAS_COLUMN = "has_column"
    HAS_ALIAS = "has_alias"


class EdgeDirection(StrEnum):
    IN = "in"
    OUT = "out"


class LineageLevel(StrEnum):
    TABLE = "table"
    COLUMN = "column"
