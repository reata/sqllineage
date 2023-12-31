from ...helpers import assert_table_lineage_equal


def test_swapping_partitions():
    """
    See https://www.vertica.com/docs/10.0.x/HTML/Content/Authoring/AdministratorsGuide/Partitions/SwappingPartitions.htm
    for language specification
    """
    assert_table_lineage_equal(
        "SELECT swap_partitions_between_tables('staging', 'min-range-value', 'max-range-value', 'target')",
        {"staging"},
        {"target"},
    )
