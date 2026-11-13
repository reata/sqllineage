from collections.abc import Iterator

from sqllineage.core.metadata.resolution.catalog import RelationCatalog
from sqllineage.core.metadata.resolution.models import (
    ResolutionDiagnostic,
    ResolvedRelation,
    ResolvedScope,
    ScopeModel,
    ScopeWildcard,
)
from sqllineage.core.metadata.resolution.rules.column_bind import bind_columns
from sqllineage.core.metadata.resolution.rules.derived_relation import ScopeColumns
from sqllineage.core.metadata.resolution.rules.wildcard import expand_wildcards
from sqllineage.core.models import Table


def resolve_scope(
    scope: ScopeModel, catalog: RelationCatalog
) -> tuple[ResolvedScope, list[ResolutionDiagnostic]]:
    """
    Resolve the scope of a statement: expand wildcards, infer the columns of CTEs and subqueries, and bind
    unqualified columns, in that order, for every SELECT of the statement.

    Table columns come from ``catalog``, which is expected to be prefetched. Nothing is raised for what can't be
    resolved: wildcards are kept as-is and columns left unbound, with a diagnostic where it helps.
    """
    columns = ScopeColumns(catalog)
    resolved = ResolvedScope()
    diagnostics: list[ResolutionDiagnostic] = []
    for head in _selects(scope):
        branches = [head, *head.union]

        # wildcard
        expansions = expand_wildcards(branches, columns.column_names)
        for branch in branches:
            for item in branch.select:
                if isinstance(item, ScopeWildcard):
                    expansion = expansions[id(item)]
                    # like in the lineage graph, an expanded column is qualified by its relation
                    resolved.wildcards[item.column] = (
                        None
                        if expansion is None
                        else [f"{name}.{column}" for name, column in expansion]
                    )
        if any(expansion is None for expansion in expansions.values()):
            diagnostics.extend(_unexpanded_wildcards(branches, catalog))

        # derived_relation
        for branch in branches:
            for relation in branch.relations:
                if (relation_columns := columns.relation_columns(relation)) is not None:
                    resolved.relations[relation.relation] = ResolvedRelation(
                        relation.relation, *relation_columns
                    )

        # column_binding
        for branch in branches:
            bindings, bind_diagnostics = bind_columns(branch, columns.column_names)
            resolved.bindings.update(bindings)
            diagnostics.extend(bind_diagnostics)

    return resolved, diagnostics


def _selects(scope: ScopeModel) -> Iterator[ScopeModel]:
    """
    Every SELECT reachable from ``scope``, through UNION branches, CTEs and subqueries. The branches of a UNION are
    only yielded through the first one.
    """
    seen: set[int] = set()
    pending = [scope]
    while pending:
        head = pending.pop()
        if id(head) in seen:
            continue
        seen.add(id(head))
        yield head
        for branch in [head, *head.union]:
            seen.add(id(branch))
            pending.extend(
                relation.scope
                for relation in branch.relations
                if relation.scope is not None
            )


def _unexpanded_wildcards(
    branches: list[ScopeModel], catalog: RelationCatalog
) -> list[ResolutionDiagnostic]:
    tables: list[Table] = []
    misses = catalog.misses
    for branch in branches:
        for item in branch.select:
            if not isinstance(item, ScopeWildcard):
                continue
            for relation in branch.relations:
                if item.qualifier not in (None, relation.name):
                    continue
                if (
                    isinstance(relation.relation, Table)
                    and relation.relation in misses
                    and relation.relation not in tables
                ):
                    tables.append(relation.relation)
    return [
        ResolutionDiagnostic(
            kind="unexpanded_wildcard",
            message=f"no columns known for {table}, the wildcard is kept as-is",
            table=table,
        )
        for table in tables
    ]
