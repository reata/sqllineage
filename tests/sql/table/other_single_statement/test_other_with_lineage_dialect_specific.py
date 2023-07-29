import pytest

from ....helpers import assert_table_lineage_equal


@pytest.mark.parametrize("dialect", ["hive"])
def test_alter_table_exchange_partition(dialect: str):
    """
    See https://cwiki.apache.org/confluence/display/Hive/Exchange+Partition for language manual
    """
    assert_table_lineage_equal(
        "ALTER TABLE tab1 EXCHANGE PARTITION (pt='part1') WITH TABLE tab2",
        {"tab2"},
        {"tab1"},
        dialect=dialect,
    )
