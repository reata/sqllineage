import pytest

from ...helpers import assert_table_lineage_equal


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


@pytest.mark.parametrize("dialect", ["snowflake"])
def test_alter_table_swap_partition(dialect: str):
    """
    See https://docs.snowflake.com/en/sql-reference/sql/alter-table for language manual
    Note swap is not a keyword in sqlparse, we'll skip testing for it.
    """
    assert_table_lineage_equal(
        "ALTER TABLE tab1 SWAP WITH tab2",
        {"tab2"},
        {"tab1"},
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", ["vertica"])
def test_swapping_partitions(dialect: str):
    """
    See https://www.vertica.com/docs/10.0.x/HTML/Content/Authoring/AdministratorsGuide/Partitions/SwappingPartitions.htm
    for language specification
    """
    assert_table_lineage_equal(
        "SELECT swap_partitions_between_tables('staging', 'min-range-value', 'max-range-value', 'target')",
        {"staging"},
        {"target"},
        dialect=dialect,
    )
