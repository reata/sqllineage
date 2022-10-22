from sqllineage.runner import LineageRunner
from .helpers import assert_table_lineage_equal


def test_use():
    assert_table_lineage_equal("USE db1")


def test_table_name_case():
    assert_table_lineage_equal(
        """insert overwrite table tab_a
select * from tab_b
union all
select * from TAB_B""",
        {"tab_b"},
        {"tab_a"},
    )


def test_create():
    assert_table_lineage_equal("CREATE TABLE tab1 (col1 STRING)", None, {"tab1"})


def test_create_if_not_exist():
    assert_table_lineage_equal(
        "CREATE TABLE IF NOT EXISTS tab1 (col1 STRING)", None, {"tab1"}
    )


def test_create_bucket_table():
    assert_table_lineage_equal(
        "CREATE TABLE tab1 USING parquet CLUSTERED BY (col1) INTO 500 BUCKETS",
        None,
        {"tab1"},
    )


def test_create_as():
    assert_table_lineage_equal(
        "CREATE TABLE tab1 AS SELECT * FROM tab2", {"tab2"}, {"tab1"}
    )


def test_create_as_with_parenthesis_around_select_statement():
    assert_table_lineage_equal(
        "CREATE TABLE tab1 AS (SELECT * FROM tab2)", {"tab2"}, {"tab1"}
    )


def test_create_as_with_parenthesis_around_table_name():
    assert_table_lineage_equal(
        "CREATE TABLE tab1 AS SELECT * FROM (tab2)", {"tab2"}, {"tab1"}
    )


def test_create_as_with_parenthesis_around_both():
    assert_table_lineage_equal(
        "CREATE TABLE tab1 AS (SELECT * FROM (tab2))", {"tab2"}, {"tab1"}
    )


def test_create_like():
    assert_table_lineage_equal("CREATE TABLE tab1 LIKE tab2", {"tab2"}, {"tab1"})


def test_create_select():
    assert_table_lineage_equal(
        "CREATE TABLE tab1 SELECT * FROM tab2", {"tab2"}, {"tab1"}
    )


def test_create_after_drop():
    assert_table_lineage_equal(
        "DROP TABLE IF EXISTS tab1; CREATE TABLE IF NOT EXISTS tab1 (col1 STRING)",
        None,
        {"tab1"},
    )


def test_create_using_serde():
    # Check https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-RowFormats&SerDe
    # here with is not an indicator for CTE
    assert_table_lineage_equal(
        """CREATE TABLE apachelog (
  host STRING,
  identity STRING,
  user STRING,
  time STRING,
  request STRING,
  status STRING,
  size STRING,
  referer STRING,
  agent STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
  "input.regex" = "([^]*) ([^]*) ([^]*) (-|\\[^\\]*\\]) ([^ \"]*|\"[^\"]*\") (-|[0-9]*) (-|[0-9]*)(?: ([^ \"]*|\".*\") ([^ \"]*|\".*\"))?"
)
STORED AS TEXTFILE""",  # noqa
        None,
        {"apachelog"},
    )


def test_bucket_with_using_parenthesis():
    assert_table_lineage_equal(
        """CREATE TABLE tbl1 (col1 VARCHAR)
  WITH (bucketed_on = array['col1'], bucket_count = 256);""",
        None,
        {"tbl1"},
    )


def test_update():
    assert_table_lineage_equal(
        "UPDATE tab1 SET col1='val1' WHERE col2='val2'", None, {"tab1"}
    )


def test_update_with_join():
    assert_table_lineage_equal(
        "UPDATE tab1 a INNER JOIN tab2 b ON a.col1=b.col1 SET a.col2=b.col2",
        {"tab2"},
        {"tab1"},
    )


def test_copy_from_table():
    assert_table_lineage_equal(
        "COPY tab1 FROM tab2",
        {"tab2"},
        {"tab1"},
    )


def test_drop():
    assert_table_lineage_equal("DROP TABLE IF EXISTS tab1", None, None)


def test_drop_with_comment():
    assert_table_lineage_equal(
        """--comment
DROP TABLE IF EXISTS tab1""",
        None,
        None,
    )


def test_drop_after_create():
    assert_table_lineage_equal(
        "CREATE TABLE IF NOT EXISTS tab1 (col1 STRING);DROP TABLE IF EXISTS tab1",
        None,
        None,
    )


def test_drop_tmp_tab_after_create():
    sql = """create table tab_a as select * from tab_b;
insert overwrite table tab_c select * from tab_a;
drop table tab_a;"""
    assert_table_lineage_equal(sql, {"tab_b"}, {"tab_c"})


def test_new_create_tab_as_tmp_table():
    sql = """create table tab_a as select * from tab_b;
create table tab_c as select * from tab_a;"""
    assert_table_lineage_equal(sql, {"tab_b"}, {"tab_c"})


def test_alter_table_rename():
    assert_table_lineage_equal("alter table tab1 rename to tab2;", None, None)


def test_rename_table():
    """
    This syntax is MySQL specific:
     https://dev.mysql.com/doc/refman/8.0/en/rename-table.html
    """
    assert_table_lineage_equal("rename table tab1 to tab2", None, None)


def test_rename_tables():
    assert_table_lineage_equal("rename table tab1 to tab2, tab3 to tab4", None, None)


def test_alter_table_exchange_partition():
    """
    See https://cwiki.apache.org/confluence/display/Hive/Exchange+Partition for language manual
    """
    assert_table_lineage_equal(
        "alter table tab1 exchange partition(pt='part1') with table tab2",
        {"tab2"},
        {"tab1"},
    )


def test_swapping_partitions():
    """
    See https://www.vertica.com/docs/10.0.x/HTML/Content/Authoring/AdministratorsGuide/Partitions/SwappingPartitions.htm
    for language specification
    """
    assert_table_lineage_equal(
        "select swap_partitions_between_tables('staging', 'min-range-value', 'max-range-value', 'target')",
        {"staging"},
        {"target"},
    )


def test_alter_target_table_name():
    assert_table_lineage_equal(
        "insert overwrite tab1 select * from tab2; alter table tab1 rename to tab3;",
        {"tab2"},
        {"tab3"},
    )


def test_refresh_table():
    assert_table_lineage_equal("refresh table tab1", None, None)


def test_cache_table():
    assert_table_lineage_equal("cache table tab1", None, None)


def test_uncache_table():
    assert_table_lineage_equal("uncache table tab1", None, None)


def test_uncache_table_if_exists():
    assert_table_lineage_equal("uncache table if exists tab1", None, None)


def test_truncate_table():
    assert_table_lineage_equal("truncate table tab1", None, None)


def test_delete_from_table():
    assert_table_lineage_equal("delete from table tab1", None, None)


def test_lateral_view_using_json_tuple():
    sql = """INSERT OVERWRITE TABLE foo
SELECT sc.id, q.item0, q.item1
FROM bar sc
LATERAL VIEW json_tuple(sc.json, 'key1', 'key2') q AS item0, item1"""
    assert_table_lineage_equal(sql, {"bar"}, {"foo"})


def test_lateral_view_outer():
    sql = """INSERT OVERWRITE TABLE foo
SELECT sc.id, q.col1
FROM bar sc
LATERAL VIEW OUTER explode(sc.json_array) q AS col1"""
    assert_table_lineage_equal(sql, {"bar"}, {"foo"})


def test_show_create_table():
    assert_table_lineage_equal("show create table tab1", None, None)


def test_split_statements():
    sql = "SELECT * FROM tab1; SELECT * FROM tab2;"
    assert len(LineageRunner(sql).statements()) == 2


def test_split_statements_with_heading_and_ending_new_line():
    sql = "\nSELECT * FROM tab1;\nSELECT * FROM tab2;\n"
    assert len(LineageRunner(sql).statements()) == 2


def test_split_statements_with_comment():
    sql = """SELECT 1;

-- SELECT 2;"""
    assert len(LineageRunner(sql).statements()) == 1


def test_statements_trim_comment():
    comment = "------------------\n"
    sql = "select * from dual;"
    assert LineageRunner(comment + sql).statements(strip_comments=True)[0] == sql


def test_split_statements_with_show_create_table():
    sql = """SELECT 1;

SHOW CREATE TABLE tab1;"""
    assert len(LineageRunner(sql).statements()) == 2


def test_split_statements_with_desc():
    sql = """SELECT 1;

DESC tab1;"""
    assert len(LineageRunner(sql).statements()) == 2
