from sqllineage.utils.entities import ColumnQualifierTuple

from ...helpers import assert_column_lineage_equal


def test_column_update_with_single_table():
    sql = """
    update
        public.tgt_tbl1 as t
    set
        name  = s.name,
        email = s.address
    from
        public.src_tbl1 as s
    where
        s.id = t.id
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("name", "public.src_tbl1"),
                ColumnQualifierTuple("name", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("address", "public.src_tbl1"),
                ColumnQualifierTuple("email", "public.tgt_tbl1"),
            ),
        ],
        test_sqlparse=False,
    )


def test_column_update_with_cte():
    sql = """
    with
        s as (
            select name, address from public.src_tbl1
        )
    update
        public.tgt_tbl1 as t
    set
        name  = s.name,
        email = s.address
    from
        s
    where
        s.id = t.id
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("name", "public.src_tbl1"),
                ColumnQualifierTuple("name", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("address", "public.src_tbl1"),
                ColumnQualifierTuple("email", "public.tgt_tbl1"),
            ),
        ],
        test_sqlparse=False,
    )


def test_column_update_with_multi_tables():
    sql = """
    update
        public.tgt_tbl1 as t
    set
        name  = s1.name,
        email = s2.email
    from
        public.src_tbl1 as s1
        join
            public.src_tbl2 as s2
        on
            s1.id = s2.id
    where
        s1.id = t.id
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("name", "public.src_tbl1"),
                ColumnQualifierTuple("name", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("email", "public.src_tbl2"),
                ColumnQualifierTuple("email", "public.tgt_tbl1"),
            ),
        ],
        test_sqlparse=False,
    )
    sql = """
    update
        public.tgt_tbl1 as t
    set
        name = s1.name,
        email = s2.email
    from
        public.src_tbl1 as s1,
        public.src_tbl2 as s2
    where
        s1.id = s2.id and
        s1.id = t.id
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("name", "public.src_tbl1"),
                ColumnQualifierTuple("name", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("email", "public.src_tbl2"),
                ColumnQualifierTuple("email", "public.tgt_tbl1"),
            ),
        ],
        test_sqlparse=False,
    )


def test_column_update_with_subquery():
    sql = """
    update
        public.tgt_tbl1 as t
    set
        name  = s.name,
        email = s.email
    from
        (
            select
                s1.id,
                s1.name,
                s2.email
            from
                public.src_tbl1 as s1
                join
                    public.src_tbl2 as s2
                on
                    s1.id = s2.id
        ) as s
    where
        s.id = t.id
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("name", "public.src_tbl1"),
                ColumnQualifierTuple("name", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("email", "public.src_tbl2"),
                ColumnQualifierTuple("email", "public.tgt_tbl1"),
            ),
        ],
        test_sqlparse=False,
    )
