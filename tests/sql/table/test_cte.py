from ...helpers import assert_table_lineage_equal


def test_with_select():
    assert_table_lineage_equal("WITH tab1 AS (SELECT 1) SELECT * FROM tab1")


def test_with_select_one():
    assert_table_lineage_equal(
        "WITH wtab1 AS (SELECT * FROM schema1.tab1) SELECT * FROM wtab1",
        {"schema1.tab1"},
    )


def test_with_select_many():
    assert_table_lineage_equal(
        """WITH
cte1 AS (SELECT a, b FROM tab1),
cte2 AS (SELECT c, d FROM tab2)
SELECT b, d FROM cte1 JOIN cte2
WHERE cte1.a = cte2.c""",
        {"tab1", "tab2"},
    )


def test_with_select_many_reference():
    assert_table_lineage_equal(
        """WITH
cte1 AS (SELECT a, b FROM tab1),
cte2 AS (SELECT a, count(*) AS cnt FROM cte1 GROUP BY a)
SELECT a, b, cnt FROM cte1 JOIN cte2
WHERE cte1.a = cte2.a""",
        {"tab1"},
    )


def test_with_select_using_alias():
    assert_table_lineage_equal(
        "WITH wtab1 AS (SELECT * FROM schema1.tab1) SELECT * FROM wtab1 wt",
        {"schema1.tab1"},
    )


def test_with_select_join_table_with_same_name():
    assert_table_lineage_equal(
        "WITH wtab1 AS (SELECT * FROM schema1.tab1) SELECT * FROM wtab1 CROSS JOIN db.wtab1",
        {"schema1.tab1", "db.wtab1"},
    )


def test_with_insert():
    assert_table_lineage_equal(
        "WITH tab1 AS (SELECT * FROM tab2) INSERT INTO tab3 SELECT * FROM tab1",
        {"tab2"},
        {"tab3"},
    )


def test_with_insert_in_query():
    assert_table_lineage_equal(
        "INSERT INTO tab3 WITH tab1 AS (SELECT * FROM tab2) SELECT * FROM tab1",
        {"tab2"},
        {"tab3"},
    )


def test_cte_and_union():
    sql = """WITH cte_1 AS (select col1 from tab1)
SELECT col2 from tab2
UNION
SELECT col3 from cte_1"""
    assert_table_lineage_equal(
        sql,
        {"tab1", "tab2"},
    )


def test_cte_and_union_but_not_selecting_from_cte():
    # issue #398
    sql = """WITH cte_1 AS (select col1 from tab1)
SELECT col2 from tab2
UNION
SELECT col3 from tab3"""
    assert_table_lineage_equal(
        sql,
        {"tab1", "tab2", "tab3"},
    )


def test_cte_within_subquery():
    sql = """SELECT sq.col1
FROM (WITH cte1 AS (SELECT col1 FROM tab1)
      SELECT col1
      FROM cte1
               INNER JOIN tab2 ON cte1.col1 = tab2.col1) AS sq"""
    assert_table_lineage_equal(
        sql,
        {"tab1", "tab2"},
    )


def test_cte_within_cte():
    sql = """WITH cte1 AS
         (WITH cte2 AS
                   (SELECT id
                    FROM tab1)
          SELECT id
          FROM cte2)
SELECT id
FROM cte1"""
    assert_table_lineage_equal(sql, {"tab1"})
