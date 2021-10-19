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
    sql = """INSERT OVERWRITE TABLE tab1
SELECT tab2.col1 AS col2
FROM tab2"""
    assert_column_lineage_equal(sql, [("tab2.col1", "tab1.col2")])


def test_select_column_wildcard():
    sql = """INSERT OVERWRITE TABLE tab1
SELECT *
FROM tab2"""
    assert_column_lineage_equal(sql, [("tab2.*", "tab1.*")])
    sql = """INSERT OVERWRITE TABLE tab1
SELECT *
FROM tab2 a
         INNER JOIN tab3 b
                    ON a.id = b.id"""
    assert_column_lineage_equal(sql, [("tab2.*", "tab1.*"), ("tab3.*", "tab1.*")])


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


def test_select_column_using_function_with_complex_parameter():
    sql = """INSERT OVERWRITE TABLE tab1
SELECT if(col1 = 'foo' AND col2 = 'bar', 1, 0) AS flag
FROM tab2"""
    assert_column_lineage_equal(
        sql, [("tab2.col1", "tab1.flag"), ("tab2.col2", "tab1.flag")]
    )


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
SELECT CASE WHEN col1 = 1 THEN 'V1' WHEN col1 = 2 THEN 'V2' END
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [
            (
                "tab2.col1",
                "tab1.CASE WHEN col1 = 1 THEN 'V1' WHEN col1 = 2 THEN 'V2' END",
            ),
        ],
    )
    sql = """INSERT OVERWRITE TABLE tab1
SELECT CASE WHEN col1 = 1 THEN 'V1' WHEN col1 = 2 THEN 'V2' END AS col2
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


def test_select_column_in_subquery():
    sql = """INSERT OVERWRITE TABLE tab1
SELECT col1
FROM (SELECT col1 FROM tab2) dt"""
    assert_column_lineage_equal(sql, [("tab2.col1", "tab1.col1")])
    sql = """INSERT OVERWRITE TABLE tab1
SELECT col1
FROM (SELECT col1, col2 FROM tab2) dt"""
    assert_column_lineage_equal(sql, [("tab2.col1", "tab1.col1")])
    sql = """INSERT OVERWRITE TABLE tab1
SELECT col1
FROM (SELECT col1 FROM tab2)"""
    assert_column_lineage_equal(sql, [("tab2.col1", "tab1.col1")])


def test_select_column_from_table_join():
    sql = """INSERT OVERWRITE TABLE tab1
SELECT tab2.col1,
       tab3.col2
FROM tab2
         INNER JOIN tab3
                    ON tab2.id = tab3.id"""
    assert_column_lineage_equal(
        sql, [("tab2.col1", "tab1.col1"), ("tab3.col2", "tab1.col2")]
    )
    sql = """INSERT OVERWRITE TABLE tab1
SELECT tab2.col1 AS col3,
       tab3.col2 AS col4
FROM tab2
         INNER JOIN tab3
                    ON tab2.id = tab3.id"""
    assert_column_lineage_equal(
        sql, [("tab2.col1", "tab1.col3"), ("tab3.col2", "tab1.col4")]
    )
    sql = """INSERT OVERWRITE TABLE tab1
SELECT a.col1 AS col3,
       b.col2 AS col4
FROM tab2 a
         INNER JOIN tab3 b
                    ON a.id = b.id"""
    assert_column_lineage_equal(
        sql, [("tab2.col1", "tab1.col3"), ("tab3.col2", "tab1.col4")]
    )


def test_select_column_without_table_prefix_from_table_join():
    sql = """INSERT OVERWRITE TABLE tab1
SELECT col1
FROM tab2 a
         INNER JOIN tab3 b
                    ON a.id = b.id"""
    assert_column_lineage_equal(sql, [("col1", "tab1.col1")])


def test_select_column_from_same_table_multiple_time_using_different_alias():
    sql = """INSERT OVERWRITE TABLE tab1
SELECT a.col1 AS col2,
       b.col1 AS col3
FROM tab2 a
         JOIN tab2 b
              ON a.parent_id = b.id"""
    assert_column_lineage_equal(
        sql, [("tab2.col1", "tab1.col2"), ("tab2.col1", "tab1.col3")]
    )


def test_comment_after_column_comma_first():
    sql = """INSERT OVERWRITE TABLE tab1
    SELECT
    a.col1
    --, a.col2
    , a.col3
    FROM
    tab2 a
    """
    assert_column_lineage_equal(
        sql, [("tab2.col1", "tab1.col1"), ("tab2.col3", "tab1.col3")]
    )


def test_comment_after_column_comma_last():
    sql = """INSERT OVERWRITE TABLE tab1
    SELECT
    a.col1,
    -- a.col2,
    a.col3
    FROM
    tab2 a
    """
    assert_column_lineage_equal(
        sql, [("tab2.col1", "tab1.col1"), ("tab2.col3", "tab1.col3")]
    )


def test_cast_with_comparison():
    sql = """
    INSERT OVERWRITE TABLE tab1
    select cast(col1=1 as int) col1, col2=col3 col2
    from tab2;
    """
    assert_column_lineage_equal(
        sql,
        [
            ("tab2.col1", "tab1.col1"),
            ("tab2.col2", "tab1.col2"),
            ("tab2.col3", "tab1.col2"),
        ],
    )


def test_window_function_in_subquery():
    sql = """
    insert into tab1
    select rn from (
        select
            ROW_NUMBER() OVER (PARTITION BY col1, col2) rn
        from tab2
    ) sub
    where rn = 1
    ;
    """
    assert_column_lineage_equal(
        sql, [("tab2.col1", "tab1.rn"), ("tab2.col2", "tab1.rn")]
    )
