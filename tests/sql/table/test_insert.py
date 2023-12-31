from ...helpers import assert_table_lineage_equal


def test_insert_into_values():
    assert_table_lineage_equal("INSERT INTO tab1 VALUES (1, 2)", set(), {"tab1"})


def test_insert_into_values_with_subquery():
    assert_table_lineage_equal(
        "INSERT INTO tab1 VALUES (1, (SELECT max(id) FROM tab2))",
        {"tab2"},
        {"tab1"},
    )


def test_insert_into_values_with_multiple_subquery():
    assert_table_lineage_equal(
        "INSERT INTO tab1 VALUES ((SELECT max(id) FROM tab2), (SELECT max(id) FROM tab3))",
        {"tab2", "tab3"},
        {"tab1"},
    )


def test_insert_into_values_with_multiple_subquery_in_multiple_row():
    assert_table_lineage_equal(
        "INSERT INTO tab1 VALUES (1, (SELECT max(id) FROM tab2)), (2, (SELECT max(id) FROM tab3))",
        {"tab2", "tab3"},
        {"tab1"},
    )


def test_insert_into_select():
    assert_table_lineage_equal(
        "INSERT INTO tab1 SELECT * FROM tab2;",
        {"tab2"},
        {"tab1"},
    )


def test_non_reserved_keyword_as_target():
    assert_table_lineage_equal(
        "INSERT INTO host SELECT col1, col2 FROM segment",
        {"segment"},
        {"host"},
        test_sqlparse=False,
    )


def test_insert_into_qualified_table_with_parenthesized_query():
    """
    For sqlparse, it will work if:
        1) table in unqualified
    OR  2) query is not surrounded by parenthesis
    With both in the game, it breaks.
    """
    sql = """INSERT INTO default.tab2
    (SELECT *
    FROM tab1)"""
    assert_table_lineage_equal(sql, {"tab1"}, {"default.tab2"}, test_sqlparse=False)
