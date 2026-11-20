from __future__ import annotations

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

    #: the columns of each relation whose columns are known, by relation
    relations: dict[Table | SubQuery, ResolvedRelation] = field(default_factory=dict)
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


@dataclass
class ScopeWildcard:
    """
    A wildcard in a SELECT list: ``*``, or ``t.*`` when ``qualifier`` is set.
    """

    #: the wildcard column, used as the key in :attr:`ResolvedScope.wildcards`
    column: Column
    qualifier: str | None = None
    except_columns: list[str] = field(default_factory=list)


@dataclass
class ScopeRelation:
    """
    A relation in the FROM clause of a scope, under the name it's referred to by (its alias, or its name).
    """

    name: str
    relation: Table | SubQuery
    #: for a subquery or CTE, the scope that defines its columns
    scope: ScopeModel | None = None


@dataclass
class ScopeModel:
    """
    The parser-independent view of one SELECT that the resolver works on.
    """

    relations: list[ScopeRelation] = field(default_factory=list)
    #: the output columns in order: a column name, or a wildcard
    select: list[str | ScopeWildcard] = field(default_factory=list)
    #: unqualified column references, to bind to one of the relations
    unqualified_columns: list[Column] = field(default_factory=list)
    #: names of the columns joined with USING or NATURAL JOIN
    using_columns: set[str] = field(default_factory=set)
    #: the other branches of a UNION with this SELECT
    union: list[ScopeModel] = field(default_factory=list)
