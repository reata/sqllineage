from sqllineage.utils.entities import ColumnQualifierTuple
from ...helpers import assert_column_lineage_equal


def test_column_top_level_lateral_ref():
    sql = """
    insert into public.tgt_tbl1
    select
        name               as user_name,
        user_name || email as id         -- lateral ref
    from
        public.src_tbl1
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("name", "public.src_tbl1"),
                ColumnQualifierTuple("user_name", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("name", "public.src_tbl1"),
                ColumnQualifierTuple("id", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("email", "public.src_tbl1"),
                ColumnQualifierTuple("id", "public.tgt_tbl1"),
            ),
        ],
    )


def test_column_lateral_ref_within_subquery():
    sql = """
    insert into public.tgt_tbl1
    select
        sq.name
    from
        (
            select
                id || name as alias1,
                alias1 || email as name
            from
                public.src_tbl1
        ) as sq
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("id", "public.src_tbl1"),
                ColumnQualifierTuple("name", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("name", "public.src_tbl1"),
                ColumnQualifierTuple("name", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("email", "public.src_tbl1"),
                ColumnQualifierTuple("name", "public.tgt_tbl1"),
            ),
        ],
    )

    sql = """
    insert into public.tgt_tbl1
    select
        sq.name
    from
        (
            select
                st1.id || st1.name as alias1,
                alias1 || st2.email as name
            from
                public.src_tbl1 as st1
                join
                    public.src_tbl2 as st2
                on
                    st1.id = st2.id
        ) as sq
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("id", "public.src_tbl1"),
                ColumnQualifierTuple("name", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("name", "public.src_tbl1"),
                ColumnQualifierTuple("name", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("email", "public.src_tbl2"),
                ColumnQualifierTuple("name", "public.tgt_tbl1"),
            ),
        ],
    )
