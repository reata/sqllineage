from sqllineage.utils.entities import ColumnQualifierTuple
from ...helpers import assert_column_lineage_equal


def test_column_cross_stmts_with_similar_subquery_alias():
    sql = """
        insert into public.tgt_tbl1 ( id )
        with
            sq1 as ( select id from public.src_tbl1 ),
            sq2 as ( select id from public.src_tbl2 ),
            sq3 as ( select id from public.src_tbl3 )
        select sq1.id from sq1
        union all
        select sq2.id from sq2
        union all
        select sq3.id from sq3;
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("id", "public.src_tbl1"),
                ColumnQualifierTuple("id", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("id", "public.src_tbl2"),
                ColumnQualifierTuple("id", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("id", "public.src_tbl3"),
                ColumnQualifierTuple("id", "public.tgt_tbl1"),
            ),
        ],
    )

    sql = """
        insert into public.tgt_tbl1 ( id )
        select sq1.id from ( select id from public.src_tbl1 ) as sq1
        union all
        select sq2.id from ( select id from public.src_tbl2 ) as sq2
        union all
        select sq3.id from ( select id from public.src_tbl3 ) as sq3;
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("id", "public.src_tbl1"),
                ColumnQualifierTuple("id", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("id", "public.src_tbl2"),
                ColumnQualifierTuple("id", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("id", "public.src_tbl3"),
                ColumnQualifierTuple("id", "public.tgt_tbl1"),
            ),
        ],
    )

    sql = """
        insert into public.tgt_tbl1 ( id )
        select sq.id from ( select id from public.src_tbl1 ) as sq
        union all
        select sq.id from ( select id from public.src_tbl2 ) as sq
        union all
        select sq.id from ( select id from public.src_tbl3 ) as sq;
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("id", "public.src_tbl1"),
                ColumnQualifierTuple("id", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("id", "public.src_tbl2"),
                ColumnQualifierTuple("id", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("id", "public.src_tbl3"),
                ColumnQualifierTuple("id", "public.tgt_tbl1"),
            ),
        ],
        skip_graph_check=True,
    )
