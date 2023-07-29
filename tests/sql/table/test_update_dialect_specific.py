import pytest

from ...helpers import assert_table_lineage_equal


@pytest.mark.parametrize("dialect", ["mysql"])
def test_update_with_join(dialect: str):
    assert_table_lineage_equal(
        "UPDATE tab1 a INNER JOIN tab2 b ON a.col1=b.col1 SET a.col2=b.col2",
        {"tab2"},
        {"tab1"},
        dialect=dialect,
    )
