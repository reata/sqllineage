import pytest
from tests.helpers import assert_column_lineage_metadata_service

from sqllineage.core.metadata.dummy import DummyMetaDataProvider
from sqllineage.exceptions import InvalidSyntaxException
from sqllineage.runner import LineageRunner
from sqllineage.utils.entities import ColumnQualifierTuple


test_schemas = {
    "db1.table1": ["id", "a", "b", "c", "d"],
    "db2.table2": ["id", "h", "i", "j", "k"],
    "db3.table3": ["pk", "p", "q", "r"],
}


def test_select_column_from_tables():
    service = DummyMetaDataProvider(test_schemas)

    sql = """insert into table db.tbl
select t1.id, a as x, b, h, i as y
from db1.table1 t1
join db2.table2 t2 on t1.id = t2.id
"""
    assert_column_lineage_metadata_service(
        sql,
        service,
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
        dialect="sparksql",
    )

    sql = """insert into table db.tbl
    select a, b as x, h, i as y, p, q, pk as z
    from db1.table1 t1
    left join db2.table2 t2 on t1.id = t2.id
    right join db3.table3 t3 on t2.id = t3.pk
    """
    assert_column_lineage_metadata_service(
        sql,
        service,
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
        dialect="sparksql",
    )


def test_select_column_from_subqueries():
    service = DummyMetaDataProvider(test_schemas)

    sql = """insert into table db.tbl
select a, b as x, h, y
from (select a, b, c from db1.table1) t1
full join (select h, i, j as y from db2.table2) t2
on t1.id = t2.id
"""
    assert_column_lineage_metadata_service(
        sql,
        service,
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
        dialect="sparksql",
    )

    sql = """insert into table db.tbl
select a, b as x, h, y, pk as z, p
from (select id, a, b, c from db1.table1) t1
join (select id, h, i as y, j from db2.table2) t2 on t1.id = t2.id
left join (select pk, p, q from db3.table3) t3 on t1.id = t3.pk
"""
    assert_column_lineage_metadata_service(
        sql,
        service,
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
        dialect="sparksql",
    )


def test_select_column_from_table_subquery():
    service = DummyMetaDataProvider(test_schemas)

    sql = """insert into table db.tbl
select a as x, b, q, pk as y
from db1.table1 t1
right join (select pk, p, q from db3.table3) t3
on t1.id = t3.pk
"""
    assert_column_lineage_metadata_service(
        sql,
        service,
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
        dialect="sparksql",
    )

    sql = """insert into table db.tbl
select a, x, h as y, i, p, z
from (select id, a, b as x, c from db1.table1) t1
join db2.table2 t2 on t1.id = t2.id
left join (select pk, p, q as z from db3.table3) t3 on t2.id = t3.pk
"""
    assert_column_lineage_metadata_service(
        sql,
        service,
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
        dialect="sparksql",
    )


def test_select_column_from_tempview_view_subquery():
    service = DummyMetaDataProvider(test_schemas)

    sql = """
create or replace temporary view test_view
as
select id, a from db1.table1
;

insert into table db.tbl
select test_view.id, a as x, h as y, i, p, z
from test_view
join db2.table2 t2 on test_view.id = t2.id
left join (select pk, p, q as z from db3.table3) t3 on t2.id = t3.pk
"""
    assert_column_lineage_metadata_service(
        sql,
        service,
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
        dialect="sparksql",
    )


def test_sqlparse_exception():
    service = DummyMetaDataProvider(test_schemas)
    sql = """insert into table db.tbl
select id
from db1.table1 t1
join db2.table2 t2 on t1.id = t2.id
"""

    with pytest.raises(
        InvalidSyntaxException,
        match="id is not allowed from more than one table or subquery",
    ):
        lr = LineageRunner(sql, metadata_service=service)
        col_lineage = lr.get_column_lineage()
        for e in col_lineage:
            print(e)
