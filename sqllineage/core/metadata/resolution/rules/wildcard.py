from collections.abc import Callable

from sqllineage.core.metadata.resolution.model import (
    ScopeModel,
    ScopeRelation,
    ScopeWildcard,
)

#: returns the known column names of a relation of a scope, or None if they're unknown
RelationColumns = Callable[[ScopeModel, ScopeRelation], list[str] | None]

#: a column of an expanded wildcard: the name of the relation it comes from, and the column name
ExpandedColumn = tuple[str, str]


def expand_wildcards(
    scopes: list[ScopeModel], columns_of: RelationColumns
) -> dict[int, list[ExpandedColumn] | None]:
    """
    Expand the wildcards in the SELECT lists of ``scopes``, the branches of one UNION (or a single SELECT).

    Each wildcard expands to the columns of the relations it selects from, without the columns in its EXCEPT
    clause. Expansion is atomic: if any wildcard of any branch can't be expanded, none of them is and every wildcard
    maps to None.

    Returns the expansion of each wildcard, keyed by ``id()`` of its :class:`ScopeWildcard`.
    """
    expansions: dict[int, list[ExpandedColumn] | None] = {}
    for scope in scopes:
        for item in scope.select:
            if isinstance(item, ScopeWildcard):
                expansions[id(item)] = _expand(scope, item, columns_of)
    if any(expansion is None for expansion in expansions.values()):
        return {key: None for key in expansions}
    return expansions


def _expand(
    scope: ScopeModel, wildcard: ScopeWildcard, columns_of: RelationColumns
) -> list[ExpandedColumn] | None:
    relations = [
        relation
        for relation in scope.relations
        if wildcard.qualifier is None or relation.name == wildcard.qualifier
    ]
    if not relations:
        return None
    names: list[ExpandedColumn] = []
    for relation in relations:
        columns = columns_of(scope, relation)
        if not columns:
            return None
        names.extend(
            (relation.name, column)
            for column in columns
            if column not in wildcard.except_columns
        )
    return names
