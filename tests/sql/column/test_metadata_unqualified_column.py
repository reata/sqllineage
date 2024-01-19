import pytest

from sqllineage.core.metadata_provider import MetaDataProvider
from sqllineage.utils.entities import ColumnQualifierTuple
from ...helpers import assert_column_lineage_equal, generate_metadata_providers


providers = generate_metadata_providers(
    {
        "db1.table1": ["id", "a", "b", "c", "d"],
        "db2.table2": ["id", "h", "i", "j", "k"],
        "db3.table3": ["pk", "p", "q", "r"],
    }
)


@pytest.mark.parametrize("provider", providers)
def test_select_column_from_tables(provider: MetaDataProvider):
    sql = """insert into db.tbl
select t1.id, a as x, b, h, i as y
from db1.table1 t1
join db2.table2 t2 on t1.id = t2.id
"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("id", "db1.table1"),
                ColumnQualifierTuple("id", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("a", "db1.table1"),
                ColumnQualifierTuple("x", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("b", "db1.table1"),
                ColumnQualifierTuple("b", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("h", "db2.table2"),
                ColumnQualifierTuple("h", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("i", "db2.table2"),
                ColumnQualifierTuple("y", "db.tbl"),
            ),
        ],
        metadata_provider=provider,
    )

    sql = """insert into db.tbl
    select a, b as x, h, i as y, p, q, pk as z
    from db1.table1 t1
    left join db2.table2 t2 on t1.id = t2.id
    right join db3.table3 t3 on t2.id = t3.pk
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("a", "db1.table1"),
                ColumnQualifierTuple("a", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("b", "db1.table1"),
                ColumnQualifierTuple("x", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("h", "db2.table2"),
                ColumnQualifierTuple("h", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("i", "db2.table2"),
                ColumnQualifierTuple("y", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("p", "db3.table3"),
                ColumnQualifierTuple("p", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("q", "db3.table3"),
                ColumnQualifierTuple("q", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("pk", "db3.table3"),
                ColumnQualifierTuple("z", "db.tbl"),
            ),
        ],
        metadata_provider=provider,
    )


@pytest.mark.parametrize("provider", providers)
def test_select_column_from_subqueries(provider: MetaDataProvider):
    sql = """insert into db.tbl
select a, b as x, h, y
from (select a, b, c from db1.table1) t1
full join (select h, i, j as y from db2.table2) t2
on t1.id = t2.id
"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("a", "db1.table1"),
                ColumnQualifierTuple("a", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("b", "db1.table1"),
                ColumnQualifierTuple("x", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("h", "db2.table2"),
                ColumnQualifierTuple("h", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("j", "db2.table2"),
                ColumnQualifierTuple("y", "db.tbl"),
            ),
        ],
        metadata_provider=provider,
    )

    sql = """insert into db.tbl
select a, b as x, h, y, pk as z, p
from (select id, a, b, c from db1.table1) t1
join (select id, h, i as y, j from db2.table2) t2 on t1.id = t2.id
left join (select pk, p, q from db3.table3) t3 on t1.id = t3.pk
"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("a", "db1.table1"),
                ColumnQualifierTuple("a", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("b", "db1.table1"),
                ColumnQualifierTuple("x", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("h", "db2.table2"),
                ColumnQualifierTuple("h", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("i", "db2.table2"),
                ColumnQualifierTuple("y", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("p", "db3.table3"),
                ColumnQualifierTuple("p", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("pk", "db3.table3"),
                ColumnQualifierTuple("z", "db.tbl"),
            ),
        ],
        metadata_provider=provider,
    )


@pytest.mark.parametrize("provider", providers)
def test_select_column_from_table_subquery(provider: MetaDataProvider):
    sql = """insert into db.tbl
select a as x, b, q, pk as y
from db1.table1 t1
right join (select pk, p, q from db3.table3) t3
on t1.id = t3.pk
"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("a", "db1.table1"),
                ColumnQualifierTuple("x", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("b", "db1.table1"),
                ColumnQualifierTuple("b", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("pk", "db3.table3"),
                ColumnQualifierTuple("y", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("q", "db3.table3"),
                ColumnQualifierTuple("q", "db.tbl"),
            ),
        ],
        metadata_provider=provider,
    )

    sql = """insert into db.tbl
select a, x, h as y, i, p, z
from (select id, a, b as x, c from db1.table1) t1
join db2.table2 t2 on t1.id = t2.id
left join (select pk, p, q as z from db3.table3) t3 on t2.id = t3.pk
"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("a", "db1.table1"),
                ColumnQualifierTuple("a", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("b", "db1.table1"),
                ColumnQualifierTuple("x", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("h", "db2.table2"),
                ColumnQualifierTuple("y", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("i", "db2.table2"),
                ColumnQualifierTuple("i", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("p", "db3.table3"),
                ColumnQualifierTuple("p", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("q", "db3.table3"),
                ColumnQualifierTuple("z", "db.tbl"),
            ),
        ],
        metadata_provider=provider,
    )


@pytest.mark.parametrize("provider", providers)
def test_select_column_from_tempview_view_subquery(provider: MetaDataProvider):
    sql = """
create or replace view test_view
as
select id, a from db1.table1
;

insert into db.tbl
select test_view.id, a as x, h as y, i, p, z
from test_view
join db2.table2 t2 on test_view.id = t2.id
left join (select pk, p, q as z from db3.table3) t3 on t2.id = t3.pk
"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("id", "db1.table1"),
                ColumnQualifierTuple("id", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("a", "db1.table1"),
                ColumnQualifierTuple("x", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("h", "db2.table2"),
                ColumnQualifierTuple("y", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("i", "db2.table2"),
                ColumnQualifierTuple("i", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("p", "db3.table3"),
                ColumnQualifierTuple("p", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("q", "db3.table3"),
                ColumnQualifierTuple("z", "db.tbl"),
            ),
        ],
        metadata_provider=provider,
    )
