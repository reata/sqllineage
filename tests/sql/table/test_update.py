from ...helpers import assert_table_lineage_equal


def test_update():
    assert_table_lineage_equal(
        "UPDATE tab1 SET col1='val1' WHERE col2='val2'", None, {"tab1"}
    )


def test_update_from():
    assert_table_lineage_equal(
        """UPDATE tab2
SET tab2.col2 = tab1.col2 FROM tab1
WHERE tab2.col1 = tab1.col1""",
        {"tab1"},
        {"tab2"},
    )
