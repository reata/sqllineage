import pytest

from .helpers import assert_table_lineage_equal


"""
This test class will contain all the tests for testing 'Insert Queries' where the dialect is not ANSI.
"""


@pytest.mark.parametrize("dialect", ["hive", "sparksql"])
def test_insert_overwrite_with_keyword_table(dialect: str):
    assert_table_lineage_equal(
        "INSERT OVERWRITE TABLE tab1 SELECT col1 FROM tab2",
        {"tab2"},
        {"tab1"},
        dialect=dialect,
    )


@pytest.mark.parametrize("dialect", ["hive", "sparksql"])
def test_insert_overwrite_from_self(dialect: str):
    assert_table_lineage_equal(
        """INSERT OVERWRITE TABLE foo
SELECT col FROM foo
WHERE flag IS NOT NULL""",
        {"foo"},
        {"foo"},
        dialect=dialect,
    )


@pytest.mark.parametrize("dialect", ["hive", "sparksql"])
def test_insert_overwrite_from_self_with_join(dialect: str):
    assert_table_lineage_equal(
        """INSERT OVERWRITE TABLE tab_1
SELECT tab_2.col_a from tab_2
JOIN tab_1
ON tab_1.col_a = tab_2.cola""",
        {"tab_1", "tab_2"},
        {"tab_1"},
        dialect=dialect,
    )


@pytest.mark.parametrize("dialect", ["hive", "sparksql"])
def test_insert_into_with_keyword_table(dialect: str):
    assert_table_lineage_equal(
        "INSERT INTO TABLE tab1 VALUES (1, 2)", set(), {"tab1"}, dialect=dialect
    )


@pytest.mark.parametrize("dialect", ["hive", "sparksql"])
def test_insert_into_partitions(dialect: str):
    assert_table_lineage_equal(
        "INSERT INTO TABLE tab1 PARTITION (par1=1) SELECT * FROM tab2",
        {"tab2"},
        {"tab1"},
        dialect=dialect,
    )


def test_insert_overwrite():
    assert_table_lineage_equal(
        "INSERT OVERWRITE tab1 SELECT * FROM tab2",
        {"tab2"},
        {"tab1"},
        dialect="sparksql",
    )


def test_insert_overwrite_values():
    assert_table_lineage_equal(
        "INSERT OVERWRITE tab1 VALUES ('val1', 'val2'), ('val3', 'val4')",
        {},
        {"tab1"},
        dialect="sparksql",
    )
