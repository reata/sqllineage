from .helpers import assert_table_lineage_equal


def test_with_select():
    assert_table_lineage_equal("WITH tab1 AS (SELECT 1) SELECT * FROM tab1")


def test_with_select_one():
    assert_table_lineage_equal(
        "WITH wtab1 AS (SELECT * FROM schema1.tab1) SELECT * FROM wtab1",
        {"schema1.tab1"},
    )


def test_with_select_one_without_as():
    # AS in CTE is negligible in SparkSQL, however it is required in MySQL. See below reference
    # https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-cte.html
    # https://dev.mysql.com/doc/refman/8.0/en/with.html
    assert_table_lineage_equal(
        "WITH wtab1 (SELECT * FROM schema1.tab1) SELECT * FROM wtab1",
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


def test_with_insert_overwrite():
    assert_table_lineage_equal(
        "WITH tab1 AS (SELECT * FROM tab2) INSERT OVERWRITE tab3 SELECT * FROM tab1",
        {"tab2"},
        {"tab3"},
    )


def test_with_insert_plus_keyword_table():
    assert_table_lineage_equal(
        "WITH tab1 AS (SELECT * FROM tab2) INSERT INTO TABLE tab3 SELECT * FROM tab1",
        {"tab2"},
        {"tab3"},
    )


def test_with_insert_overwrite_plus_keyword_table():
    assert_table_lineage_equal(
        "WITH tab1 AS (SELECT * FROM tab2) INSERT OVERWRITE TABLE tab3 SELECT * FROM tab1",
        {"tab2"},
        {"tab3"},
    )
