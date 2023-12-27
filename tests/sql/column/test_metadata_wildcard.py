from tests.helpers import assert_wildcard_lineage

from sqllineage.core.metadata.dummy import DummyMetaDataProvider
from sqllineage.utils.entities import ColumnQualifierTuple


test_schemas = {
    "db.tbl_x": ["id", "a", "b"],
    "db.tbl_y": ["id", "h", "i"],
    "db.tbl_z": ["pk", "s", "t"],
}


def test_select_single_table():
    provider = DummyMetaDataProvider(test_schemas)

    sql = """create temporary view test_v
    select *
    from db.tbl_x
    """
    assert_wildcard_lineage(
        sql,
        provider,
        [
            (
                ColumnQualifierTuple("id", "db.tbl_x"),
                ColumnQualifierTuple("id", "<default>.test_v"),
            ),
            (
                ColumnQualifierTuple("a", "db.tbl_x"),
                ColumnQualifierTuple("a", "<default>.test_v"),
            ),
            (
                ColumnQualifierTuple("b", "db.tbl_x"),
                ColumnQualifierTuple("b", "<default>.test_v"),
            ),
        ],
    )

    sql = """create temporary view test_v
    select id, b
    from (
        select *
        from (
            select a, b, id from db.tbl_x
        ) t1
    ) t2
    """
    assert_wildcard_lineage(
        sql,
        provider,
        [
            (
                ColumnQualifierTuple("id", "db.tbl_x"),
                ColumnQualifierTuple("id", "<default>.test_v"),
            ),
            (
                ColumnQualifierTuple("b", "db.tbl_x"),
                ColumnQualifierTuple("b", "<default>.test_v"),
            ),
        ],
    )

    sql = """create temporary view test_v
    select a, b
    from (
        select *, row_number() over (partition by id) as rn
        from db.tbl_x t
    ) t1
    where rn = 1
    """
    assert_wildcard_lineage(
        sql,
        provider,
        [
            (
                ColumnQualifierTuple("a", "db.tbl_x"),
                ColumnQualifierTuple("a", "<default>.test_v"),
            ),
            (
                ColumnQualifierTuple("b", "db.tbl_x"),
                ColumnQualifierTuple("b", "<default>.test_v"),
            ),
        ],
    )

    sql = """create temporary view test_v
    select a, b
    from (
        select *, row_number() over (partition by a) as rn
        from db.tbl_x t
    ) t1
    where rn = 1
    """
    assert_wildcard_lineage(
        sql,
        provider,
        [
            (
                ColumnQualifierTuple("a", "db.tbl_x"),
                ColumnQualifierTuple("a", "<default>.test_v"),
            ),
            (
                ColumnQualifierTuple("b", "db.tbl_x"),
                ColumnQualifierTuple("b", "<default>.test_v"),
            ),
        ],
    )


def test_select_table_join_table():
    provider = DummyMetaDataProvider(test_schemas)

    sql = """create temporary view test_v
    select a, id, h
    from (
        select x.a, y.* from db.tbl_x x
        join db.tbl_y y on x.id = y.id
    ) t
    """
    assert_wildcard_lineage(
        sql,
        provider,
        [
            (
                ColumnQualifierTuple("a", "db.tbl_x"),
                ColumnQualifierTuple("a", "<default>.test_v"),
            ),
            (
                ColumnQualifierTuple("id", "db.tbl_y"),
                ColumnQualifierTuple("id", "<default>.test_v"),
            ),
            (
                ColumnQualifierTuple("h", "db.tbl_y"),
                ColumnQualifierTuple("h", "<default>.test_v"),
            ),
        ],
    )

    sql = """create temporary view test_v
    select a, id, h
    from (
        select x.*, y.h from db.tbl_x x
        join db.tbl_y y on x.id = y.id
    ) t
    """
    assert_wildcard_lineage(
        sql,
        provider,
        [
            (
                ColumnQualifierTuple("a", "db.tbl_x"),
                ColumnQualifierTuple("a", "<default>.test_v"),
            ),
            (
                ColumnQualifierTuple("id", "db.tbl_x"),
                ColumnQualifierTuple("id", "<default>.test_v"),
            ),
            (
                ColumnQualifierTuple("h", "db.tbl_y"),
                ColumnQualifierTuple("h", "<default>.test_v"),
            ),
        ],
    )

    sql = """create temporary view test_v
    select a, h
    from (
        select x.*, y.* from db.tbl_x x
        join db.tbl_y y on x.id = y.id
    ) t
    """
    assert_wildcard_lineage(
        sql,
        provider,
        [
            (
                ColumnQualifierTuple("a", "db.tbl_x"),
                ColumnQualifierTuple("a", "<default>.test_v"),
            ),
            (
                ColumnQualifierTuple("h", "db.tbl_y"),
                ColumnQualifierTuple("h", "<default>.test_v"),
            ),
        ],
    )

    sql = """create temporary view test_v
    select *
    from (
        select x.*, z.* from db.tbl_x x
        join db.tbl_z z on x.id = z.pk
    ) t
    """
    assert_wildcard_lineage(
        sql,
        provider,
        [
            (
                ColumnQualifierTuple("a", "db.tbl_x"),
                ColumnQualifierTuple("a", "<default>.test_v"),
            ),
            (
                ColumnQualifierTuple("b", "db.tbl_x"),
                ColumnQualifierTuple("b", "<default>.test_v"),
            ),
            (
                ColumnQualifierTuple("id", "db.tbl_x"),
                ColumnQualifierTuple("id", "<default>.test_v"),
            ),
            (
                ColumnQualifierTuple("pk", "db.tbl_z"),
                ColumnQualifierTuple("pk", "<default>.test_v"),
            ),
            (
                ColumnQualifierTuple("s", "db.tbl_z"),
                ColumnQualifierTuple("s", "<default>.test_v"),
            ),
            (
                ColumnQualifierTuple("t", "db.tbl_z"),
                ColumnQualifierTuple("t", "<default>.test_v"),
            ),
        ],
    )

    sql = """create temporary view test_v
    select x.*, y.h, y.i
    from (select * from db.tbl_x where a = 0) x
    join (select *
          from (
              select *, row_number() over (partition by i) as rn from db.tbl_y
          ) t1
          where rn = 1
    ) y
    on x.id = y.id
    """
    assert_wildcard_lineage(
        sql,
        provider,
        [
            (
                ColumnQualifierTuple("a", "db.tbl_x"),
                ColumnQualifierTuple("a", "<default>.test_v"),
            ),
            (
                ColumnQualifierTuple("b", "db.tbl_x"),
                ColumnQualifierTuple("b", "<default>.test_v"),
            ),
            (
                ColumnQualifierTuple("id", "db.tbl_x"),
                ColumnQualifierTuple("id", "<default>.test_v"),
            ),
            (
                ColumnQualifierTuple("h", "db.tbl_y"),
                ColumnQualifierTuple("h", "<default>.test_v"),
            ),
            (
                ColumnQualifierTuple("i", "db.tbl_y"),
                ColumnQualifierTuple("i", "<default>.test_v"),
            ),
        ],
    )


def test_multiple_statements():
    provider = DummyMetaDataProvider(test_schemas)

    sql = """create temporary view test_x
    select a, b
    from (
        select *, row_number() over (partition by id) as rn
        from db.tbl_x t
    ) t1
    where rn = 1
    ;

    create temporary view test_z
    select *
    from db.tbl_z
    ;

    insert into table db.tbl
    select t1.*, t2.h, t3.*
    from test_x t1
    join (select * from db.tbl_y where i > 0) t2 on t1.id = t2.id
    left join test_z t3 on t2.id = t3.pk
    """
    assert_wildcard_lineage(
        sql,
        provider,
        [
            (
                ColumnQualifierTuple("a", "db.tbl_x"),
                ColumnQualifierTuple("a", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("b", "db.tbl_x"),
                ColumnQualifierTuple("b", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("h", "db.tbl_y"),
                ColumnQualifierTuple("h", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("pk", "db.tbl_z"),
                ColumnQualifierTuple("pk", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("s", "db.tbl_z"),
                ColumnQualifierTuple("s", "db.tbl"),
            ),
            (
                ColumnQualifierTuple("t", "db.tbl_z"),
                ColumnQualifierTuple("t", "db.tbl"),
            ),
        ],
    )
