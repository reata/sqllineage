from ...helpers import assert_table_lineage_equal


def test_merge_into_using_table():
    sql = """MERGE INTO target
USING src ON target.k = src.k
WHEN MATCHED THEN UPDATE SET target.v = src.v"""
    assert_table_lineage_equal(sql, {"src"}, {"target"})


def test_merge_into_using_subquery():
    sql = """MERGE INTO target USING (select k, max(v) as v from src group by k) AS b ON target.k = b.k
WHEN MATCHED THEN UPDATE SET target.v = b.v
WHEN NOT MATCHED THEN INSERT (k, v) VALUES (b.k, b.v)"""
    assert_table_lineage_equal(sql, {"src"}, {"target"})


def test_merge_using_cte_subquery():
    sql = """MERGE INTO tgt t
USING (
    WITH base AS (
        SELECT
            id, max(value) AS value
        FROM src
        GROUP BY id
    )
    SELECT
        id, value
    FROM base
) s
ON t.id = s.id
WHEN MATCHED THEN
UPDATE SET t.value = s.value"""
    assert_table_lineage_equal(
        sql,
        {"src"},
        {"tgt"},
    )


def test_merge_into_insert_one_column():
    sql = """MERGE INTO target
USING src ON target.k = src.k
WHEN NOT MATCHED THEN INSERT VALUES (src.k)"""
    assert_table_lineage_equal(sql, {"src"}, {"target"})


def test_merge_with_union_in_subquery_and_join():
    sql = """MERGE INTO tgt t
USING (SELECT s1.id, baz.value
       FROM (SELECT id
             FROM foo
             UNION ALL
             SELECT id
             FROM bar) s1
                CROSS JOIN baz) s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET t.value = s.value
"""
    assert_table_lineage_equal(sql, {"foo", "bar", "baz"}, {"tgt"})
