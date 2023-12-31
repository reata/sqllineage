import pytest

from sqllineage.core.metadata_provider import MetaDataProvider
from sqllineage.utils.entities import ColumnQualifierTuple
from ...helpers import assert_column_lineage_equal, generate_metadata_providers


providers = generate_metadata_providers(
    {
        "db.tbl_x": ["id", "a", "b"],
        "db.tbl_y": ["id", "h", "i"],
        "db.tbl_z": ["pk", "s", "t"],
    }
)


@pytest.mark.parametrize("provider", providers)
def test_select_single_table_wildcard(provider: MetaDataProvider):
    sql = """insert into test_v
    select *
    from db.tbl_x
    """
    assert_column_lineage_equal(
        sql,
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
        metadata_provider=provider,
    )


@pytest.mark.parametrize("provider", providers)
def test_select_single_table_wildcard_in_subquery(provider: MetaDataProvider):
    sql = """insert into test_v
    select id, b
    from (
        select *
        from (
            select a, b, id from db.tbl_x
        ) t1
    ) t2
    """
    assert_column_lineage_equal(
        sql,
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
        metadata_provider=provider,
    )


@pytest.mark.parametrize("provider", providers)
def test_select_single_table_partial_wildcard_in_subquery(provider: MetaDataProvider):
    sql = """insert into test_v
    select a, b
    from (
        select *, row_number() over (partition by id) as rn
        from db.tbl_x t
    ) t1
    where rn = 1
    """
    assert_column_lineage_equal(
        sql,
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
        metadata_provider=provider,
    )
    sql = """insert into test_v
    select a, b
    from (
        select *, row_number() over (partition by a) as rn
        from db.tbl_x t
    ) t1
    where rn = 1
    """
    assert_column_lineage_equal(
        sql,
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
        metadata_provider=provider,
    )


@pytest.mark.parametrize("provider", providers)
def test_select_table_join_partial_wildcard_at_last(provider: MetaDataProvider):
    sql = """insert into test_v
    select a, id, h
    from (
        select x.a, y.* from db.tbl_x x
        join db.tbl_y y on x.id = y.id
    ) t
    """
    assert_column_lineage_equal(
        sql,
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
        metadata_provider=provider,
    )


@pytest.mark.parametrize("provider", providers)
def test_select_table_join_partial_wildcard_at_beginning(provider: MetaDataProvider):
    sql = """insert into test_v
    select a, id, h
    from (
        select x.*, y.h from db.tbl_x x
        join db.tbl_y y on x.id = y.id
    ) t
    """
    assert_column_lineage_equal(
        sql,
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
        metadata_provider=provider,
    )


@pytest.mark.parametrize("provider", providers)
def test_select_table_join_multiple_wildcards(provider: MetaDataProvider):
    sql = """insert into test_v
    select a, h
    from (
        select x.*, y.* from db.tbl_x x
        join db.tbl_y y on x.id = y.id
    ) t
    """
    assert_column_lineage_equal(
        sql,
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
        metadata_provider=provider,
    )


@pytest.mark.parametrize("provider", providers)
def test_select_table_join_multiple_wildcards_merged_at_top_level(
    provider: MetaDataProvider,
):
    sql = """insert into test_v
    select *
    from (
        select x.*, z.* from db.tbl_x x
        join db.tbl_z z on x.id = z.pk
    ) t
    """
    assert_column_lineage_equal(
        sql,
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
        metadata_provider=provider,
    )


@pytest.mark.parametrize("provider", providers)
def test_select_table_join_multiple_wildcards_at_different_level(
    provider: MetaDataProvider,
):
    sql = """insert into test_v
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
    assert_column_lineage_equal(
        sql,
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
        metadata_provider=provider,
    )


@pytest.mark.parametrize("provider", providers)
def test_wildcard_reference_from_previous_statements(provider: MetaDataProvider):
    sql = """create table test_x as
    select a, b
    from (
        select *, row_number() over (partition by id) as rn
        from db.tbl_x t
    ) t1
    where rn = 1
    ;

    create table test_z as
    select *
    from db.tbl_z
    ;

    insert into db.tbl
    select t1.*, t2.h, t3.*
    from test_x t1
    join (select * from db.tbl_y where i > 0) t2 on t1.id = t2.id
    left join test_z t3 on t2.id = t3.pk
    """
    assert_column_lineage_equal(
        sql,
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
        metadata_provider=provider,
    )
