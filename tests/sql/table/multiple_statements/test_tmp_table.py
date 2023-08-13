from ....helpers import assert_table_lineage_equal


def test_drop_tmp_tab_after_create():
    sql = """CREATE TABLE tab_a AS
SELECT *
FROM tab_b;
INSERT INTO tab_c
SELECT *
FROM tab_a;
DROP TABLE tab_a;"""
    assert_table_lineage_equal(sql, {"tab_b"}, {"tab_c"})


def test_new_create_tab_as_tmp_table():
    sql = """CREATE TABLE tab_a AS
SELECT *
FROM tab_b;
CREATE TABLE tab_c AS
SELECT *
FROM tab_a;"""
    assert_table_lineage_equal(sql, {"tab_b"}, {"tab_c"})


def test_create_after_drop():
    assert_table_lineage_equal(
        "DROP TABLE IF EXISTS tab1; CREATE TABLE IF NOT EXISTS tab1 AS SELECT 1",
        None,
        {"tab1"},
    )


def test_drop_after_create():
    assert_table_lineage_equal(
        "CREATE TABLE IF NOT EXISTS tab1 AS SELECT 1; DROP TABLE IF EXISTS tab1",
        None,
        None,
    )


def test_alter_target_table_name():
    assert_table_lineage_equal(
        "INSERT INTO tab1 SELECT * FROM tab2; ALTER TABLE tab1 RENAME TO tab3;",
        {"tab2"},
        {"tab3"},
    )
