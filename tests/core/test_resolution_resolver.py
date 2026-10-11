import itertools

from sqllineage.core.metadata.dummy import DummyMetaDataProvider
from sqllineage.core.metadata.resolution import (
    CachingRelationCatalog,
    ColumnSource,
    ScopeModel,
    ScopeRelation,
    ScopeWildcard,
    resolve_scope,
)
from sqllineage.core.models import Column, SourcePosition, SubQuery, Table

_select_ids = itertools.count()

METADATA = {
    "db.a": ["id", "a1"],
    "db.b": ["id", "b1"],
    "db.t": ["col1", "col2", "col3"],
}


def catalog(metadata: dict[str, list[str]] | None = None) -> CachingRelationCatalog:
    return CachingRelationCatalog(
        DummyMetaDataProvider(METADATA if metadata is None else metadata)
    )


def table(name: str, alias: str) -> ScopeRelation:
    return ScopeRelation(alias, Table(name, alias=alias))


def subquery(alias: str, scope: ScopeModel) -> ScopeRelation:
    return ScopeRelation(alias, SubQuery(None, "", alias, SourcePosition(1, 1)), scope)


def wildcard(qualifier: str | None = None, *except_columns: str) -> ScopeWildcard:
    # like in the lineage graph, each wildcard belongs to the dataset its SELECT produces
    column = Column("*")
    column.parent = Table(f"tgt.select_{next(_select_ids)}")
    return ScopeWildcard(column, qualifier, list(except_columns))


def test_wildcard_expands_with_qualifier():
    star = wildcard()
    scope = ScopeModel(relations=[table("db.t", "t")], select=[star])

    resolved, diagnostics = resolve_scope(scope, catalog())
    assert resolved.wildcards[star.column] == ["t.col1", "t.col2", "t.col3"]
    assert resolved.relations[Table("db.t")].source == ColumnSource.CATALOG
    assert diagnostics == []


def test_wildcard_from_relation_without_alias():
    inner_star, outer_star = wildcard(), wildcard()
    inner = ScopeModel(relations=[table("db.t", "db.t")], select=[inner_star])
    scope = ScopeModel(relations=[subquery("s", inner)], select=[outer_star])

    resolved, _ = resolve_scope(scope, catalog())
    assert resolved.wildcards[inner_star.column] == [
        "db.t.col1",
        "db.t.col2",
        "db.t.col3",
    ]
    assert resolved.wildcards[outer_star.column] == ["s.col1", "s.col2", "s.col3"]


def test_wildcard_over_join():
    star, a_star = wildcard(), wildcard("a")
    scope = ScopeModel(
        relations=[table("db.a", "a"), table("db.b", "b")], select=[star, a_star]
    )

    resolved, _ = resolve_scope(scope, catalog())
    assert resolved.wildcards[star.column] == ["a.id", "a.a1", "b.id", "b.b1"]
    assert resolved.wildcards[a_star.column] == ["a.id", "a.a1"]


def test_wildcard_except_columns():
    star = wildcard(None, "col2")
    scope = ScopeModel(relations=[table("db.t", "t")], select=[star])

    resolved, _ = resolve_scope(scope, catalog())
    assert resolved.wildcards[star.column] == ["t.col1", "t.col3"]


def test_wildcards_in_select_list_are_expanded_all_or_none():
    a_star, x_star = wildcard("a"), wildcard("x")
    scope = ScopeModel(
        relations=[table("db.a", "a"), table("db.unknown", "x")],
        select=[a_star, x_star],
    )

    resolved, diagnostics = resolve_scope(scope, catalog())
    assert resolved.wildcards == {a_star.column: None, x_star.column: None}
    assert [(d.kind, d.table) for d in diagnostics] == [
        ("unexpanded_wildcard", Table("db.unknown"))
    ]


def test_unexpanded_wildcard_diagnostic_once_per_table():
    scope = ScopeModel(
        relations=[table("db.unknown", "x")],
        select=[wildcard("x"), wildcard()],
    )

    _, diagnostics = resolve_scope(scope, catalog())
    assert [(d.kind, d.table) for d in diagnostics] == [
        ("unexpanded_wildcard", Table("db.unknown"))
    ]


def test_wildcards_in_union_are_expanded_all_or_none():
    a_star, x_star = wildcard(), wildcard()
    scope = ScopeModel(
        relations=[table("db.a", "a")],
        select=[a_star],
        union=[ScopeModel(relations=[table("db.unknown", "x")], select=[x_star])],
    )

    resolved, _ = resolve_scope(scope, catalog())
    assert resolved.wildcards == {a_star.column: None, x_star.column: None}


def test_no_unexpanded_wildcard_diagnostic_without_metadata():
    star = wildcard()
    scope = ScopeModel(relations=[table("db.t", "t")], select=[star])

    resolved, diagnostics = resolve_scope(scope, catalog({}))
    assert resolved.wildcards[star.column] is None
    assert diagnostics == []


def test_cte_columns_are_inferred_from_its_select():
    inner_star, outer_star = wildcard(), wildcard()
    cte = ScopeModel(relations=[table("db.t", "t")], select=[inner_star])
    cte_relation = subquery("c", cte)
    scope = ScopeModel(relations=[cte_relation], select=[outer_star])

    resolved, _ = resolve_scope(scope, catalog())
    assert resolved.wildcards[inner_star.column] == ["t.col1", "t.col2", "t.col3"]
    assert resolved.wildcards[outer_star.column] == ["c.col1", "c.col2", "c.col3"]
    assert resolved.relations[cte_relation.relation].source == ColumnSource.SCOPE


def test_cte_referring_to_earlier_cte():
    c1 = ScopeModel(
        relations=[table("db.t", "t")], select=["col1", wildcard("t", "col1")]
    )
    c2 = ScopeModel(relations=[subquery("c1", c1)], select=["col3", "col1"])
    star = wildcard()
    scope = ScopeModel(relations=[subquery("c2", c2)], select=[star])

    resolved, _ = resolve_scope(scope, catalog())
    assert resolved.wildcards[star.column] == ["c2.col3", "c2.col1"]


def test_recursive_cte_columns_are_unknown():
    star = wildcard()
    cte = ScopeModel(select=[wildcard()])
    cte.relations = [subquery("c", cte)]
    scope = ScopeModel(relations=[subquery("c", cte)], select=[star])

    resolved, _ = resolve_scope(scope, catalog())
    assert resolved.wildcards[star.column] is None


def test_subquery_with_explicit_columns():
    star = wildcard()
    inner = ScopeModel(relations=[table("db.unknown", "x")], select=["c1", "c2"])
    scope = ScopeModel(relations=[subquery("s", inner)], select=[star])

    resolved, _ = resolve_scope(scope, catalog())
    assert resolved.wildcards[star.column] == ["s.c1", "s.c2"]


def test_bind_single_relation():
    column = Column("anything")
    scope = ScopeModel(
        relations=[table("db.unknown", "x")], unqualified_columns=[column]
    )

    resolved, diagnostics = resolve_scope(scope, catalog())
    assert resolved.bindings[column] == Table("db.unknown")
    assert diagnostics == []


def test_bind_column_found_in_one_relation():
    column = Column("b1")
    scope = ScopeModel(
        relations=[table("db.a", "a"), table("db.b", "b")], unqualified_columns=[column]
    )

    resolved, diagnostics = resolve_scope(scope, catalog())
    assert resolved.bindings[column] == Table("db.b")
    assert diagnostics == []


def test_bind_ambiguous_column():
    column = Column("id")
    scope = ScopeModel(
        relations=[table("db.a", "a"), table("db.b", "b")], unqualified_columns=[column]
    )

    resolved, diagnostics = resolve_scope(scope, catalog())
    assert resolved.bindings[column] is None
    assert [(d.kind, d.column) for d in diagnostics] == [("ambiguous_column", "id")]


def test_bind_using_column_is_left_for_every_relation():
    column = Column("id")
    scope = ScopeModel(
        relations=[table("db.a", "a"), table("db.b", "b")],
        unqualified_columns=[column],
        using_columns={"id"},
    )

    resolved, diagnostics = resolve_scope(scope, catalog())
    assert resolved.bindings[column] is None
    assert diagnostics == []


def test_bind_using_column_with_columns_known_for_one_relation():
    column = Column("id")
    scope = ScopeModel(
        relations=[table("db.a", "a"), table("db.c", "c")],
        unqualified_columns=[column],
        using_columns={"id"},
    )

    resolved, diagnostics = resolve_scope(scope, catalog())
    assert resolved.bindings[column] is None
    assert diagnostics == []


def test_bind_unresolved_column():
    column = Column("nope")
    scope = ScopeModel(
        relations=[table("db.a", "a"), table("db.b", "b")], unqualified_columns=[column]
    )

    resolved, diagnostics = resolve_scope(scope, catalog())
    assert resolved.bindings[column] is None
    assert [(d.kind, d.column) for d in diagnostics] == [("unresolved_column", "nope")]


def test_each_table_is_looked_up_once():
    lookups: list[str] = []

    class CountingProvider(DummyMetaDataProvider):
        def _get_table_columns(self, schema: str, table: str, **kwargs) -> list[str]:
            lookups.append(f"{schema}.{table}")
            return super()._get_table_columns(schema, table, **kwargs)

    scope = ScopeModel(
        relations=[table("db.t", "t1"), table("db.t", "t2")],
        select=[wildcard("t1"), wildcard("t2")],
        unqualified_columns=[Column("col1")],
    )

    resolve_scope(scope, CachingRelationCatalog(CountingProvider(METADATA)))
    assert lookups == ["db.t"]
