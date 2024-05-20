from ...helpers import assert_table_lineage_equal


def test_select():
    assert_table_lineage_equal("SELECT col1 FROM tab1", {"tab1"})


def test_select_with_schema():
    assert_table_lineage_equal("SELECT col1 FROM schema1.tab1", {"schema1.tab1"})


def test_select_with_schema_and_database():
    assert_table_lineage_equal(
        "SELECT col1 FROM db1.schema1.tbl1", {"db1.schema1.tbl1"}
    )


def test_select_multi_line():
    assert_table_lineage_equal(
        """SELECT col1 FROM
tab1""",
        {"tab1"},
    )


def test_select_asterisk():
    assert_table_lineage_equal("SELECT * FROM tab1", {"tab1"})


def test_select_value():
    assert_table_lineage_equal("SELECT 1")


def test_select_function():
    assert_table_lineage_equal("SELECT NOW()")


def test_select_trim_function_with_from_keyword():
    assert_table_lineage_equal("SELECT trim(BOTH '  ' FROM '  abc  ')")


def test_select_trim_function_with_from_keyword_from_source_table():
    assert_table_lineage_equal("SELECT trim(BOTH '  ' FROM col1) FROM tab1", {"tab1"})


def test_select_with_where():
    assert_table_lineage_equal(
        "SELECT * FROM tab1 WHERE col1 > val1 AND col2 = 'val2'", {"tab1"}
    )


def test_select_with_parenthesized_where():
    assert_table_lineage_equal(
        "SELECT * FROM tab1 WHERE (col1 > val1 AND col2 = 'val2')", {"tab1"}
    )


def test_select_with_comment():
    assert_table_lineage_equal("SELECT -- comment1\n col1 FROM tab1", {"tab1"})


def test_select_with_comment_after_from():
    assert_table_lineage_equal("SELECT col1\nFROM  -- comment\ntab1", {"tab1"})


def test_select_with_comment_after_join():
    assert_table_lineage_equal(
        "SELECT * FROM tab1 JOIN --comment\ntab2 ON tab1.x = tab2.x", {"tab1", "tab2"}
    )


def test_select_keyword_as_column_alias():
    # here "as" is the column alias
    assert_table_lineage_equal('SELECT 1 "as" FROM tab1', {"tab1"})
    # the following is hive specific, MySQL doesn't allow this syntax. As of now, we don't test against it
    # assert_table_lineage_equal("SELECT 1 as FROM tab1", {"tab1"})


def test_select_with_table_alias():
    assert_table_lineage_equal("SELECT 1 FROM tab1 AS alias1", {"tab1"})


def test_select_count():
    assert_table_lineage_equal("SELECT count(*) FROM tab1", {"tab1"})


def test_select_parenthesized_from_table():
    sql = "SELECT * FROM (tab1)"
    assert_table_lineage_equal(sql, {"tab1"})


def test_select_parenthesized_from_table_join():
    sql = """SELECT tab1.id
FROM (tab1
    LEFT JOIN tab2 ON tab1.id = tab2.id)"""
    assert_table_lineage_equal(sql, {"tab1", "tab2"})


def test_select_parenthesized_from_table_join_using():
    sql = """SELECT tab1.id
FROM (tab1
    LEFT JOIN tab2 USING (id))"""
    assert_table_lineage_equal(sql, {"tab1", "tab2"})


def test_select_parenthesized_from_table_join_recursive():
    sql = """SELECT tab1.id
FROM ((tab1
    LEFT JOIN tab2 ON tab1.id = tab2.id)
    LEFT JOIN tab3 ON tab1.id = tab3.id)
    """
    assert_table_lineage_equal(sql, {"tab1", "tab2", "tab3"})
    sql = """SELECT tab1.id
FROM (((tab1
    LEFT JOIN tab2 ON tab1.id = tab2.id)
    LEFT JOIN tab3 ON tab1.id = tab3.id)
    LEFT JOIN tab4 ON tab1.id = tab4.id)
    """
    assert_table_lineage_equal(sql, {"tab1", "tab2", "tab3", "tab4"})
    sql = """SELECT tab1.id
FROM ((((tab1
    LEFT JOIN tab2 ON tab1.id = tab2.id)
    LEFT JOIN tab3 ON tab1.id = tab3.id)
    LEFT JOIN tab4 ON tab1.id = tab4.id)
    LEFT JOIN tab5 ON tab1.id = tab5.id)
    """
    assert_table_lineage_equal(sql, {"tab1", "tab2", "tab3", "tab4", "tab5"})
    sql = """SELECT tab1.id
FROM (((((tab1
    LEFT JOIN tab2 ON tab1.id = tab2.id)
    LEFT JOIN tab3 ON tab1.id = tab3.id)
    LEFT JOIN tab4 ON tab1.id = tab4.id)
    LEFT JOIN tab5 ON tab1.id = tab5.id)
    LEFT JOIN tab6 ON tab1.id = tab6.id)
        """
    assert_table_lineage_equal(sql, {"tab1", "tab2", "tab3", "tab4", "tab5", "tab6"})


def test_select_subquery():
    assert_table_lineage_equal("SELECT col1 FROM (SELECT col1 FROM tab1) dt", {"tab1"})
    # with an extra space
    assert_table_lineage_equal("SELECT col1 FROM ( SELECT col1 FROM tab1) dt", {"tab1"})


def test_select_subquery_with_two_parenthesis():
    assert_table_lineage_equal(
        "SELECT col1 FROM ((SELECT col1 FROM tab1)) dt", {"tab1"}
    )


def test_select_subquery_with_more_parenthesis():
    assert_table_lineage_equal(
        "SELECT col1 FROM (((((((SELECT col1 FROM tab1))))))) dt", {"tab1"}
    )


def test_select_subquery_in_case():
    assert_table_lineage_equal(
        """SELECT
CASE WHEN (SELECT count(*) FROM tab1 WHERE col1 = 'tab2') = 1 THEN (SELECT count(*) FROM tab2) ELSE 0 END AS cnt""",
        {"tab1", "tab2"},
    )
    assert_table_lineage_equal(
        """SELECT
CASE WHEN 1 = (SELECT count(*) FROM tab1 WHERE col1 = 'tab2') THEN (SELECT count(*) FROM tab2) ELSE 0 END AS cnt""",
        {"tab1", "tab2"},
    )
    assert_table_lineage_equal(
        """
        select
            1,
            case
                when 1 = ( select count(*) from tab1 where col1 = 'tab2' )
                    then ( select count(*) from tab2 )
                else 0
            end as cnt
        """,
        {"tab1", "tab2"},
    )


def test_select_subquery_without_alias():
    """this syntax is valid in SparkSQL, not for MySQL"""
    assert_table_lineage_equal("SELECT col1 FROM (SELECT col1 FROM tab1)", {"tab1"})


def test_select_subquery_in_where_clause():
    assert_table_lineage_equal(
        """SELECT col1
FROM tab1
WHERE col1 IN (SELECT max(col1) FROM tab2)""",
        {"tab1", "tab2"},
    )


def test_select_subquery_in_function():
    assert_table_lineage_equal(
        "SELECT TO_DATE((SELECT MIN(dt) FROM tab1))",
        {"tab1"},
    )


def test_select_multiple_subquery_in_function():
    assert_table_lineage_equal(
        "SELECT LEAST((SELECT MIN(dt) FROM tab1), (SELECT MIN(dt) FROM tab2))",
        {"tab1", "tab2"},
    )


def test_select_subquery_in_function_nested():
    assert_table_lineage_equal(
        """SELECT EXPLODE(SEQUENCE(
        TO_DATE((SELECT MIN(dt) FROM tab1)),
        TO_DATE((SELECT MAX(dt) FROM tab2)),
        INTERVAL 1 DAY
)) AS result""",
        {"tab1", "tab2"},
    )


def test_select_inner_join():
    assert_table_lineage_equal("SELECT * FROM tab1 INNER JOIN tab2", {"tab1", "tab2"})


def test_select_join():
    assert_table_lineage_equal("SELECT * FROM tab1 JOIN tab2", {"tab1", "tab2"})


def test_select_left_join():
    assert_table_lineage_equal("SELECT * FROM tab1 LEFT JOIN tab2", {"tab1", "tab2"})


def test_select_left_join_with_extra_space_in_middle():
    assert_table_lineage_equal("SELECT * FROM tab1 LEFT  JOIN tab2", {"tab1", "tab2"})


def test_select_right_join():
    assert_table_lineage_equal("SELECT * FROM tab1 RIGHT JOIN tab2", {"tab1", "tab2"})


def test_select_full_outer_join():
    assert_table_lineage_equal(
        "SELECT * FROM tab1 FULL OUTER JOIN tab2", {"tab1", "tab2"}
    )


def test_select_cross_join():
    assert_table_lineage_equal("SELECT * FROM tab1 CROSS JOIN tab2", {"tab1", "tab2"})


def test_select_cross_join_with_on():
    assert_table_lineage_equal(
        "SELECT * FROM tab1 CROSS JOIN tab2 ON tab1.col1 = tab2.col2", {"tab1", "tab2"}
    )


def test_select_join_with_subquery():
    assert_table_lineage_equal(
        "SELECT col1 FROM tab1 AS a LEFT JOIN tab2 AS b ON a.id=b.tab1_id "
        "WHERE col1 = (SELECT col1 FROM tab2 WHERE id = 1)",
        {"tab1", "tab2"},
    )


def test_select_join_in_ansi89_syntax():
    assert_table_lineage_equal("SELECT * FROM tab1 a, tab2 b", {"tab1", "tab2"})


def test_select_join_in_ansi89_syntax_with_subquery():
    assert_table_lineage_equal(
        "SELECT * FROM (SELECT * FROM tab1) a, (SELECT * FROM tab2) b", {"tab1", "tab2"}
    )


def test_select_group_by():
    assert_table_lineage_equal(
        "SELECT col1, col2 FROM tab1 GROUP BY col1, col2", {"tab1"}
    )


def test_select_group_by_ordinal():
    assert_table_lineage_equal("SELECT col1, col2 FROM tab1 GROUP BY 1, 2", {"tab1"})


def test_select_from_values():
    assert_table_lineage_equal("SELECT * FROM (VALUES (1, 2))")


def test_select_from_values_newline():
    assert_table_lineage_equal("SELECT * FROM (\nVALUES (1, 2))")


def test_select_from_values_with_alias():
    assert_table_lineage_equal("SELECT * FROM (VALUES (1, 2)) AS t(col1, col2)")


def test_select_from_unnest():
    # unnest function is Presto specific
    assert_table_lineage_equal(
        "SELECT student, score FROM tests CROSS JOIN UNNEST(scores) AS t (score)",
        {"tests"},
    )


def test_select_from_unnest_parsed_as_keyword():
    # an extra space after UNNEST changes the AST structure
    assert_table_lineage_equal(
        "SELECT student, score FROM tests CROSS JOIN UNNEST (scores) AS t (score)",
        {"tests"},
    )


def test_table_name_case_insensitive():
    assert_table_lineage_equal(
        """SELECT * FROM tab_b
UNION ALL
SELECT * FROM TAB_B""",
        {"tab_b"},
    )


def test_select_union_all():
    sql = """SELECT col1
FROM tab1
UNION ALL
SELECT col1
FROM tab2
UNION ALL
SELECT col1
FROM tab3
ORDER BY col1"""
    assert_table_lineage_equal(
        sql,
        {"tab1", "tab2", "tab3"},
    )


def test_select_union_all_with_nested_subquery():
    sql = """SELECT sq1.id
FROM (SELECT id
      FROM tab1) sq1
UNION ALL
SELECT sq2.id
FROM (SELECT id
      FROM tab2) sq2"""
    assert_table_lineage_equal(sql, {"tab1", "tab2"})


def test_non_reserved_keyword_as_source():
    assert_table_lineage_equal(
        "SELECT col1, col2 FROM segment", {"segment"}, test_sqlparse=False
    )


def test_select_in_parenthesis():
    assert_table_lineage_equal("(SELECT * FROM tab1)", {"tab1"}, test_sqlparse=False)
