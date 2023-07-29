from ...helpers import assert_table_lineage_equal


def test_update():
    assert_table_lineage_equal(
        "UPDATE tab1 SET col1='val1' WHERE col2='val2'", None, {"tab1"}
    )
