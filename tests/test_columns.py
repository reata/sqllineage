from .helpers import assert_column_lineage_equal


def test_select_column():
    sql = """INSERT OVERWRITE TABLE tab1
SELECT col1
FROM tab2"""
    assert_column_lineage_equal(sql, [("tab2.col1", "tab1.col1")])
    sql = """INSERT OVERWRITE TABLE tab1
SELECT col1 AS col2
FROM tab2"""
    assert_column_lineage_equal(sql, [("tab2.col1", "tab1.col2")])


def test_select_column_wildcard():
    sql = """INSERT OVERWRITE TABLE tab1
SELECT *
FROM tab2"""
    assert_column_lineage_equal(sql, [("tab2.*", "tab1.*")])


def test_select_column_using_function():
    sql = """INSERT OVERWRITE TABLE tab1
SELECT max(col1)
FROM tab2"""
    assert_column_lineage_equal(sql, [("tab2.col1", "tab1.max(col1)")])
    sql = """INSERT OVERWRITE TABLE tab1
SELECT max(col1) AS col2
FROM tab2"""
    assert_column_lineage_equal(sql, [("tab2.col1", "tab1.col2")])

    sql = """INSERT OVERWRITE TABLE tab1
SELECT cast(col1 as timestamp)
FROM tab2"""
    assert_column_lineage_equal(sql, [("tab2.col1", "tab1.cast(col1 as timestamp)")])
    sql = """INSERT OVERWRITE TABLE tab1
SELECT cast(col1 as timestamp) as col2
FROM tab2"""
    assert_column_lineage_equal(sql, [("tab2.col1", "tab1.col2")])


def test_select_column_using_window_function():
    sql = """INSERT OVERWRITE TABLE tab1
SELECT row_number() OVER (PARTITION BY col1 ORDER BY col2 DESC) AS rnum
FROM tab2"""
    assert_column_lineage_equal(
        sql, [("tab2.col1", "tab1.rnum"), ("tab2.col2", "tab1.rnum")]
    )


def test_select_column_using_expression():
    sql = """INSERT OVERWRITE TABLE tab1
SELECT col1 + col2
FROM tab2"""
    assert_column_lineage_equal(
        sql, [("tab2.col1", "tab1.col1 + col2"), ("tab2.col2", "tab1.col1 + col2")]
    )
    sql = """INSERT OVERWRITE TABLE tab1
SELECT col1 + col2 AS col3
FROM tab2"""
    assert_column_lineage_equal(
        sql, [("tab2.col1", "tab1.col3"), ("tab2.col2", "tab1.col3")]
    )


def test_select_column_using_case_when():
    sql = """INSERT OVERWRITE TABLE tab1
SELECT CASE WHEN col1 = 1 THEN "V1" WHEN col1 = 2 THEN "V2" END
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [
            (
                "tab2.col1",
                'tab1.CASE WHEN col1 = 1 THEN "V1" WHEN col1 = 2 THEN "V2" END',
            )
        ],
    )
    sql = """INSERT OVERWRITE TABLE tab1
SELECT CASE WHEN col1 = 1 THEN "V1" WHEN col1 = 2 THEN "V2" END AS col2
FROM tab2"""
    assert_column_lineage_equal(sql, [("tab2.col1", "tab1.col2")])


def test_select_column_with_table_prefix():
    sql = """INSERT OVERWRITE TABLE tab1
SELECT tab2.col1
FROM tab2"""
    assert_column_lineage_equal(sql, [("tab2.col1", "tab1.col1")])
    sql = """INSERT OVERWRITE TABLE tab1
SELECT t.col1
FROM tab2 AS t"""
    assert_column_lineage_equal(sql, [("tab2.col1", "tab1.col1")])


def test_select_columns():
    sql = """INSERT OVERWRITE TABLE tab1
SELECT col1,
col2
FROM tab2"""
    assert_column_lineage_equal(
        sql, [("tab2.col1", "tab1.col1"), ("tab2.col2", "tab1.col2")]
    )
    sql = """INSERT OVERWRITE TABLE tab1
SELECT max(col1),
max(col2)
FROM tab2"""
    assert_column_lineage_equal(
        sql, [("tab2.col1", "tab1.max(col1)"), ("tab2.col2", "tab1.max(col2)")]
    )
