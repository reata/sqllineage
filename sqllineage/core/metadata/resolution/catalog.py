from collections.abc import Iterable
from typing import Protocol

from sqllineage.core.metadata_provider import MetaDataProvider
from sqllineage.core.models import Table


class RelationCatalog(Protocol):
    """
    Read access to the columns of tables that live in the catalog, used by the resolver.
    """

    def column_names(self, table: Table) -> list[str]:
        """
        Return the column names of the table, or an empty list if they're unknown.
        """
        ...

    @property
    def misses(self) -> frozenset[Table]:
        """
        Tables that were looked up but have no known columns.
        """
        ...


class CachingRelationCatalog:
    """
    A :class:`RelationCatalog` backed by a :class:`sqllineage.core.metadata_provider.MetaDataProvider`.

    Each table is looked up at most once, so ``FROM t a JOIN t b`` triggers a single lookup. Create one catalog per
    statement and :meth:`prefetch` the tables of the statement before resolving it, so that the resolution itself
    doesn't query the provider. Session metadata takes priority over the provider, like in
    :meth:`sqllineage.core.metadata_provider.MetaDataProvider.get_table_columns`.
    """

    def __init__(self, provider: MetaDataProvider) -> None:
        self._provider = provider
        self._columns: dict[Table, list[str]] = {}

    def prefetch(self, tables: Iterable[Table]) -> None:
        """
        Look up the columns of every table. Anything that isn't a :class:`sqllineage.core.models.Table`, like a
        subquery or path, is skipped.
        """
        for table in tables:
            if isinstance(table, Table):
                self._lookup(table)

    def column_names(self, table: Table) -> list[str]:
        """
        Return the column names of the table, or an empty list if they're unknown. A table that wasn't prefetched is
        looked up now.
        """
        return list(self._lookup(table))

    @property
    def misses(self) -> frozenset[Table]:
        """
        Tables that were looked up but have no known columns.
        """
        return frozenset(
            table for table, columns in self._columns.items() if not columns
        )

    def _lookup(self, table: Table) -> list[str]:
        if table not in self._columns:
            if not self._provider:
                # like the `if metadata_provider` checks around the existing lookups, a provider without metadata
                # isn't queried, and the table doesn't count as looked up
                return []
            self._columns[table] = [
                column.raw_name for column in self._provider.get_table_columns(table)
            ]
        return self._columns[table]
