import pytest

from .helpers import assert_table_lineage_equal

"""
This test class will contain all the tests for testing 'Other Queries' where the dialect is not ANSI.
"""


@pytest.mark.parametrize("dialect", ["hive", "sparksql"])
def test_table_name_case(dialect: str):
    assert_table_lineage_equal(
        """insert overwrite table tab_a
select * from tab_b
union all
select * from TAB_B""",
        {"tab_b"},
        {"tab_a"},
        dialect,
    )


def test_create_bucket_table():
    assert_table_lineage_equal(
        "CREATE TABLE tab1 USING parquet CLUSTERED BY (col1) INTO 500 BUCKETS",
        None,
        {"tab1"},
        "bigquery",
    )


def test_create_select():
    assert_table_lineage_equal(
        "CREATE TABLE tab1 SELECT * FROM tab2", {"tab2"}, {"tab1"}, "sparksql"
    )


def test_update_with_join():
    assert_table_lineage_equal(
        "UPDATE tab1 a INNER JOIN tab2 b ON a.col1=b.col1 SET a.col2=b.col2",
        {"tab2"},
        {"tab1"},
        "mysql",
    )


@pytest.mark.parametrize("dialect", ["hive", "sparksql"])
def test_drop_tmp_tab_after_create(dialect: str):
    sql = """create table tab_a as select * from tab_b;
insert overwrite table tab_c select * from tab_a;
drop table tab_a;"""
    assert_table_lineage_equal(sql, {"tab_b"}, {"tab_c"}, dialect)


def test_rename_table():
    """
    This syntax is MySQL specific:
     https://dev.mysql.com/doc/refman/8.0/en/rename-table.html
    """
    assert_table_lineage_equal("rename table tab1 to tab2", None, None, "mysql")


def test_rename_tables():
    assert_table_lineage_equal(
        "rename table tab1 to tab2, tab3 to tab4", None, None, "mysql"
    )


def test_alter_table_exchange_partition():
    """
    See https://cwiki.apache.org/confluence/display/Hive/Exchange+Partition for language manual
    """
    assert_table_lineage_equal(
        "alter table tab1 exchange partition(pt='part1') with table tab2",
        {"tab2"},
        {"tab1"},
        "hive",
    )


def test_alter_target_table_name():
    assert_table_lineage_equal(
        "insert overwrite tab1 select * from tab2; alter table tab1 rename to tab3;",
        {"tab2"},
        {"tab3"},
        "sparksql",
    )


def test_refresh_table():
    assert_table_lineage_equal("refresh table tab1", None, None, "sparksql")


def test_cache_table():
    assert_table_lineage_equal(
        "cache table tab1 select * from tab2", None, None, "sparksql"
    )


def test_uncache_table():
    assert_table_lineage_equal("uncache table tab1", None, None, "sparksql")


def test_uncache_table_if_exists():
    assert_table_lineage_equal("uncache table if exists tab1", None, None, "sparksql")


@pytest.mark.parametrize("dialect", ["hive", "sparksql"])
def test_lateral_view_using_json_tuple(dialect: str):
    sql = """INSERT OVERWRITE TABLE foo
SELECT sc.id, q.item0, q.item1
FROM bar sc
LATERAL VIEW json_tuple(sc.json, 'key1', 'key2') q AS item0, item1"""
    assert_table_lineage_equal(sql, {"bar"}, {"foo"}, dialect)


@pytest.mark.parametrize("dialect", ["hive", "sparksql"])
def test_lateral_view_outer(dialect: str):
    sql = """INSERT OVERWRITE TABLE foo
SELECT sc.id, q.col1
FROM bar sc
LATERAL VIEW OUTER explode(sc.json_array) q AS col1"""
    assert_table_lineage_equal(sql, {"bar"}, {"foo"}, dialect)


def test_show_create_table():
    assert_table_lineage_equal("show create table tab1", None, None, "sparksql")
