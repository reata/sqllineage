from sqllineage.utils.entities import ColumnQualifierTuple

from ...helpers import assert_column_lineage_equal


def test_merge_into_update():
    sql = """MERGE INTO target
USING src ON target.k = src.k
WHEN MATCHED THEN UPDATE SET target.v = src.v"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("v", "src"), ColumnQualifierTuple("v", "target"))],
    )


def test_merge_into_update_multiple_columns():
    sql = """MERGE INTO target
USING src ON target.k = src.k
WHEN MATCHED THEN UPDATE SET target.v = src.v, target.v1 = src.v1"""
    assert_column_lineage_equal(
        sql,
        [
            (ColumnQualifierTuple("v", "src"), ColumnQualifierTuple("v", "target")),
            (ColumnQualifierTuple("v1", "src"), ColumnQualifierTuple("v1", "target")),
        ],
    )


def test_merge_into_update_multiple_columns_with_constant():
    sql = """MERGE INTO target
USING src ON target.k = src.k
WHEN MATCHED THEN UPDATE SET target.v = src.v, target.v1 = 1"""
    assert_column_lineage_equal(
        sql, [(ColumnQualifierTuple("v", "src"), ColumnQualifierTuple("v", "target"))]
    )
    sql = """MERGE INTO target
USING src ON target.k = src.k
WHEN MATCHED THEN UPDATE SET target.v1 = 1, target.v = src.v"""
    assert_column_lineage_equal(
        sql, [(ColumnQualifierTuple("v", "src"), ColumnQualifierTuple("v", "target"))]
    )


def test_merge_into_update_multiple_match():
    sql = """MERGE INTO target
USING src ON target.k = src.k
WHEN MATCHED AND src.v0=1 THEN UPDATE SET target.v = src.v
WHEN MATCHED AND src.v0=2 THEN UPDATE SET target.v1 = src.v1"""
    assert_column_lineage_equal(
        sql,
        [
            (ColumnQualifierTuple("v", "src"), ColumnQualifierTuple("v", "target")),
            (ColumnQualifierTuple("v1", "src"), ColumnQualifierTuple("v1", "target")),
        ],
    )


def test_merge_into_insert():
    sql = """MERGE INTO target
USING src ON target.k = src.k
WHEN NOT MATCHED THEN INSERT (k, v) VALUES (src.k, src.v)"""
    assert_column_lineage_equal(
        sql,
        [
            (ColumnQualifierTuple("k", "src"), ColumnQualifierTuple("k", "target")),
            (ColumnQualifierTuple("v", "src"), ColumnQualifierTuple("v", "target")),
        ],
    )


def test_merge_into_insert_with_constant():
    sql = """MERGE INTO target
USING src ON target.k = src.k
WHEN NOT MATCHED THEN INSERT (k, v) VALUES (src.k, 1)"""
    assert_column_lineage_equal(
        sql, [(ColumnQualifierTuple("k", "src"), ColumnQualifierTuple("k", "target"))]
    )
    sql = """MERGE INTO target
USING src ON target.k = src.k
WHEN NOT MATCHED THEN INSERT (v, k) VALUES (1, src.k)"""
    assert_column_lineage_equal(
        sql, [(ColumnQualifierTuple("k", "src"), ColumnQualifierTuple("k", "target"))]
    )


def test_merge_into_insert_one_column():
    sql = """MERGE INTO target
USING src ON target.k = src.k
WHEN NOT MATCHED THEN INSERT (k) VALUES (src.k)"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("k", "src"), ColumnQualifierTuple("k", "target"))],
    )


def test_merge_into_using_subquery():
    sql = """MERGE INTO target USING (select k, max(v) as v_max from src group by k) AS b ON target.k = b.k
WHEN MATCHED THEN UPDATE SET target.v = b.v_max
WHEN NOT MATCHED THEN INSERT (k, v) VALUES (b.k, b.v_max)"""
    assert_column_lineage_equal(
        sql,
        [
            (ColumnQualifierTuple("v", "src"), ColumnQualifierTuple("v", "target")),
            (ColumnQualifierTuple("k", "src"), ColumnQualifierTuple("k", "target")),
        ],
    )
