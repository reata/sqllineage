from .helpers import assert_table_lineage_equal


def test_insert_into():
    assert_table_lineage_equal("INSERT INTO tab1 VALUES (1, 2)", set(), {"tab1"})


def test_insert_into_with_keyword_table():
    assert_table_lineage_equal("INSERT INTO TABLE tab1 VALUES (1, 2)", set(), {"tab1"})


def test_insert_into_with_columns():
    assert_table_lineage_equal(
        "INSERT INTO tab1 (col1, col2) SELECT * FROM tab2;", {"tab2"}, {"tab1"}
    )


def test_insert_into_with_columns_and_select():
    assert_table_lineage_equal(
        "INSERT INTO tab1 (col1, col2) SELECT * FROM tab2", {"tab2"}, {"tab1"}
    )


def test_insert_into_with_columns_and_select_union():
    assert_table_lineage_equal(
        "INSERT INTO tab1 (col1, col2) SELECT * FROM tab2 UNION SELECT * FROM tab3",
        {"tab2", "tab3"},
        {"tab1"},
    )


def test_insert_into_partitions():
    assert_table_lineage_equal(
        "INSERT INTO TABLE tab1 PARTITION (par1=1) SELECT * FROM tab2",
        {"tab2"},
        {"tab1"},
    )


def test_insert_overwrite():
    assert_table_lineage_equal(
        "INSERT OVERWRITE tab1 SELECT * FROM tab2", {"tab2"}, {"tab1"}
    )


def test_insert_overwrite_with_keyword_table():
    assert_table_lineage_equal(
        "INSERT OVERWRITE TABLE tab1 SELECT col1 FROM tab2", {"tab2"}, {"tab1"}
    )


def test_insert_overwrite_values():
    assert_table_lineage_equal(
        "INSERT OVERWRITE tab1 VALUES ('val1', 'val2'), ('val3', 'val4')", {}, {"tab1"}
    )


def test_insert_overwrite_from_self():
    assert_table_lineage_equal(
        """INSERT OVERWRITE TABLE foo
SELECT col FROM foo
WHERE flag IS NOT NULL""",
        {"foo"},
        {"foo"},
    )


def test_insert_overwrite_from_self_with_join():
    assert_table_lineage_equal(
        """INSERT OVERWRITE TABLE tab_1
SELECT tab_2.col_a from tab_2
JOIN tab_1
ON tab_1.col_a = tab_2.cola""",
        {"tab_1", "tab_2"},
        {"tab_1"},
    )
