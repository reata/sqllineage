from sqllineage.core.metadata.resolution.models import ResolutionDiagnostic, ScopeModel
from sqllineage.core.metadata.resolution.rules.wildcard import RelationColumns
from sqllineage.core.models import Column, SubQuery, Table


def bind_columns(
    scope: ScopeModel, columns_of: RelationColumns
) -> tuple[dict[Column, Table | SubQuery | None], list[ResolutionDiagnostic]]:
    """
    Bind each unqualified column of ``scope`` to the relation it comes from.

    With a single relation, every column comes from it. A column joined with USING or NATURAL JOIN is left unbound
    without a diagnostic, so that lineage is kept from every relation it comes from. Otherwise a column is bound to the
    only relation whose known columns include it. A column found in two or more relations is left unbound with an
    ``ambiguous_column`` diagnostic, and a column found in no relation whose columns are known with an
    ``unresolved_column`` diagnostic.
    """
    bindings: dict[Column, Table | SubQuery | None] = {}
    diagnostics: list[ResolutionDiagnostic] = []
    for column in scope.unqualified_columns:
        if len(scope.relations) == 1:
            bindings[column] = scope.relations[0].relation
            continue
        if column.raw_name in scope.using_columns:
            bindings[column] = None
            continue
        candidates = _candidates(scope, column.raw_name, columns_of)
        if len(candidates) == 1:
            bindings[column] = candidates[0]
            continue
        bindings[column] = None
        if candidates:
            diagnostics.append(
                ResolutionDiagnostic(
                    kind="ambiguous_column",
                    message=f"column {column.raw_name} is in "
                    + ", ".join(str(c) for c in candidates),
                    column=column.raw_name,
                )
            )
        else:
            diagnostics.append(
                ResolutionDiagnostic(
                    kind="unresolved_column",
                    message=f"column {column.raw_name} is not in any relation with known columns",
                    column=column.raw_name,
                )
            )
    return bindings, diagnostics


def _candidates(
    scope: ScopeModel, name: str, columns_of: RelationColumns
) -> list[Table | SubQuery]:
    candidates: list[Table | SubQuery] = []
    for relation in scope.relations:
        columns = columns_of(scope, relation)
        if columns and name in columns and relation.relation not in candidates:
            candidates.append(relation.relation)
    return candidates
