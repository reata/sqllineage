from .helpers import helper


def test_insert_into():
    helper("INSERT INTO tab1 VALUES (1, 2)", set(), {"tab1"})


def test_insert_into_with_keyword_table():
    helper("INSERT INTO TABLE tab1 VALUES (1, 2)", set(), {"tab1"})


def test_insert_into_with_columns():
    helper("INSERT INTO tab1 (col1, col2) SELECT * FROM tab2;", {"tab2"}, {"tab1"})


def test_insert_into_with_columns_and_select():
    helper("INSERT INTO tab1 (col1, col2) SELECT * FROM tab2", {"tab2"}, {"tab1"})


def test_insert_into_with_columns_and_select_union():
    helper(
        "INSERT INTO tab1 (col1, col2) SELECT * FROM tab2 UNION SELECT * FROM tab3",
        {"tab2", "tab3"},
        {"tab1"},
    )


def test_insert_into_partitions():
    helper(
        "INSERT INTO TABLE tab1 PARTITION (par1=1) SELECT * FROM tab2",
        {"tab2"},
        {"tab1"},
    )


def test_insert_overwrite():
    helper("INSERT OVERWRITE tab1 SELECT * FROM tab2", {"tab2"}, {"tab1"})


def test_insert_overwrite_with_keyword_table():
    helper("INSERT OVERWRITE TABLE tab1 SELECT col1 FROM tab2", {"tab2"}, {"tab1"})


def test_insert_overwrite_values():
    helper(
        "INSERT OVERWRITE tab1 VALUES ('val1', 'val2'), ('val3', 'val4')", {}, {"tab1"}
    )


def test_insert_overwrite_from_self():
    helper(
        """INSERT OVERWRITE TABLE tab_1
SELECT tab2.col_a from tab_2
JOIN tab_1
ON tab_1.col_a = tab_2.cola""",
        {"tab_1", "tab_2"},
        {"tab_1"},
    )


def test_with_insert():
    helper(
        "WITH tab1 AS (SELECT * FROM tab2) INSERT INTO tab3 SELECT * FROM tab1",
        {"tab2"},
        {"tab3"},
    )


def test_with_insert_overwrite():
    helper(
        "WITH tab1 AS (SELECT * FROM tab2) INSERT OVERWRITE tab3 SELECT * FROM tab1",
        {"tab2"},
        {"tab3"},
    )


def test_with_insert_plus_keyword_table():
    helper(
        "WITH tab1 AS (SELECT * FROM tab2) INSERT INTO TABLE tab3 SELECT * FROM tab1",
        {"tab2"},
        {"tab3"},
    )


def test_with_insert_overwrite_plus_keyword_table():
    helper(
        "WITH tab1 AS (SELECT * FROM tab2) INSERT OVERWRITE TABLE tab3 SELECT * FROM tab1",
        {"tab2"},
        {"tab3"},
    )
