from sqllineage.core.metadata.resolution.catalog import RelationCatalog
from sqllineage.core.metadata.resolution.model import (
    ColumnSource,
    ScopeModel,
    ScopeRelation,
    ScopeWildcard,
)
from sqllineage.core.metadata.resolution.rules.wildcard import expand_wildcards
from sqllineage.core.models import Table


class ScopeColumns:
    """
    Infer the columns of the relations of a statement: tables from the catalog, and CTEs and subqueries from the
    SELECT list of the scope that defines them.

    Scopes are visited depth first and the result is cached, so a CTE that refers to an earlier CTE is inferred after
    it. A scope that refers to itself, directly or not (a recursive CTE), has unknown columns.
    """

    def __init__(self, catalog: RelationCatalog) -> None:
        self.catalog = catalog
        self._output: dict[int, list[str] | None] = {}
        self._visiting: set[int] = set()

    def relation_columns(
        self, relation: ScopeRelation
    ) -> tuple[list[str], ColumnSource] | None:
        """
        The columns of a relation and where they come from, or None if they're unknown.
        """
        if relation.scope is not None:
            columns = self.output_columns(relation.scope)
            return None if columns is None else (columns, ColumnSource.SCOPE)
        if isinstance(relation.relation, Table):
            catalog_columns = self.catalog.column_names(relation.relation)
            if catalog_columns:
                return catalog_columns, ColumnSource.CATALOG
        return None

    def column_names(
        self, scope: ScopeModel, relation: ScopeRelation
    ) -> list[str] | None:
        columns = self.relation_columns(relation)
        return None if columns is None else columns[0]

    def output_columns(self, scope: ScopeModel) -> list[str] | None:
        """
        The output column names of a SELECT, or None if they can't be known, e.g. because a wildcard can't be
        expanded.
        """
        key = id(scope)
        if key in self._output:
            return self._output[key]
        if key in self._visiting:
            return None
        self._visiting.add(key)
        try:
            expansions = expand_wildcards([scope, *scope.union], self.column_names)
            columns: list[str] = []
            known = True
            for item in scope.select:
                if not isinstance(item, ScopeWildcard):
                    columns.append(item)
                elif (expansion := expansions[id(item)]) is not None:
                    columns.extend(column for _, column in expansion)
                else:
                    known = False
                    break
        finally:
            self._visiting.discard(key)
        self._output[key] = columns if known else None
        return self._output[key]
