from sqllineage.core.metadata.dummy import DummyMetaDataProvider
from sqllineage.core.metadata.resolution import (
    CachingRelationCatalog,
    ColumnSource,
    ResolvedRelation,
    ResolvedScope,
)
from sqllineage.core.models import Column, Table


class CountingMetaDataProvider(DummyMetaDataProvider):
    def __init__(self, metadata: dict[str, list[str]] | None = None):
        super().__init__(metadata)
        self.lookups: list[str] = []

    def _get_table_columns(self, schema: str, table: str, **kwargs) -> list[str]:
        self.lookups.append(f"{schema}.{table}")
        return super()._get_table_columns(schema, table, **kwargs)


def test_prefetch_looks_up_each_table_once():
    provider = CountingMetaDataProvider({"db.t": ["Col1", "col2"]})
    catalog = CachingRelationCatalog(provider)

    # FROM db.t a JOIN db.t b
    catalog.prefetch([Table("db.t", alias="a"), Table("db.t", alias="b")])
    assert catalog.column_names(Table("db.t")) == ["col1", "col2"]
    assert catalog.column_names(Table("db.t")) == ["col1", "col2"]
    assert provider.lookups == ["db.t"]


def test_column_names_looks_up_tables_not_prefetched():
    provider = CountingMetaDataProvider({"db.t": ["col1"]})
    catalog = CachingRelationCatalog(provider)

    assert catalog.column_names(Table("db.t")) == ["col1"]
    assert provider.lookups == ["db.t"]


def test_misses_are_tables_looked_up_without_columns():
    provider = CountingMetaDataProvider({"db.t": ["col1"]})
    catalog = CachingRelationCatalog(provider)

    catalog.prefetch([Table("db.t"), Table("db.unknown")])
    assert catalog.column_names(Table("db.unknown")) == []
    assert catalog.misses == frozenset({Table("db.unknown")})


def test_session_metadata_takes_priority_over_provider():
    provider = CountingMetaDataProvider({"db.t": ["col1"]})
    provider.register_session_metadata(Table("db.tmp"), [Column("a"), Column("b")])
    catalog = CachingRelationCatalog(provider)

    catalog.prefetch([Table("db.tmp")])
    assert catalog.column_names(Table("db.tmp")) == ["a", "b"]
    assert provider.lookups == []


def test_provider_without_metadata_is_not_queried():
    provider = CountingMetaDataProvider()
    catalog = CachingRelationCatalog(provider)

    catalog.prefetch([Table("db.t")])
    assert catalog.column_names(Table("db.t")) == []
    assert provider.lookups == []
    assert catalog.misses == frozenset()


def test_column_names_returns_a_copy():
    catalog = CachingRelationCatalog(DummyMetaDataProvider({"db.t": ["col1"]}))

    catalog.column_names(Table("db.t")).append("col2")
    assert catalog.column_names(Table("db.t")) == ["col1"]


def test_resolved_scope_defaults_are_independent():
    scope_1, scope_2 = ResolvedScope(), ResolvedScope()
    scope_1.relations["t"] = ResolvedRelation(
        Table("db.t"), ["col1"], ColumnSource.CATALOG
    )

    assert scope_2.relations == {}
    assert scope_2.bindings == {}
    assert scope_2.wildcards == {}
