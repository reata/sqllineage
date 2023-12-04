from sqllineage.utils.entities import ColumnQualifierTuple
from ...helpers import assert_column_lineage_equal


def test_column_cross_stmts_with_similar_subquery_alias():
    sql = """
        insert into public.tgt_tbl1 ( id ) with sq as ( select id from public.src_tbl1 ) select sq.id from sq;
        insert into public.tgt_tbl1 ( id ) with sq as ( select id from public.src_tbl2 ) select sq.id from sq;
        insert into public.tgt_tbl1 ( id ) with sq as ( select id from public.src_tbl3 ) select sq.id from sq;
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
        insert into public.tgt_tbl1 ( id ) select sq.id from ( select id from public.src_tbl1 ) sq;
        insert into public.tgt_tbl1 ( id ) select sq.id from ( select id from public.src_tbl2 ) sq;
        insert into public.tgt_tbl1 ( id ) select sq.id from ( select id from public.src_tbl3 ) sq;
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
