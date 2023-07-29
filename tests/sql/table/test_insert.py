from ...helpers import assert_table_lineage_equal


def test_insert_into():
    assert_table_lineage_equal("INSERT INTO tab1 VALUES (1, 2)", set(), {"tab1"})


def test_insert_into_select():
    assert_table_lineage_equal(
        "INSERT INTO tab1 SELECT * FROM tab2;",
        {"tab2"},
        {"tab1"},
    )
