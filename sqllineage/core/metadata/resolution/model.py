from dataclasses import dataclass, field
from enum import Enum
from typing import Literal

from sqllineage.core.models import Column, SubQuery, Table


class ColumnSource(Enum):
    """
    Where the columns of a resolved relation come from.
    """

    CATALOG = "catalog"
    SCOPE = "scope"
    CANDIDATES = "candidates"


@dataclass
class ResolvedRelation:
    """
    A relation of a statement, with its columns.
    """

    relation: Table | SubQuery
    columns: list[str]
    source: ColumnSource


@dataclass
class ResolvedScope:
    """
    The result of resolving the scope of a statement.
    """

    relations: dict[object, ResolvedRelation] = field(default_factory=dict)
    #: the relation each column is bound to; None means the column is left unqualified
    bindings: dict[Column, Table | SubQuery | None] = field(default_factory=dict)
    #: the column names each wildcard expands to; None means the wildcard is not expanded and kept as-is
    wildcards: dict[Column, list[str] | None] = field(default_factory=dict)


@dataclass
class ResolutionDiagnostic:
    """
    Something the resolver couldn't resolve. Diagnostics are reported, never raised.
    """

    kind: Literal["ambiguous_column", "unresolved_column", "unexpanded_wildcard"]
    message: str
    table: Table | None = None
    column: str | None = None
