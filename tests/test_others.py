from sqllineage.runner import LineageRunner
from sqllineage.utils.helpers import split
from .helpers import assert_table_lineage_equal


def test_use():
    assert_table_lineage_equal("USE db1")


def test_table_name_case():
    assert_table_lineage_equal(
        """insert into tab_a
select * from tab_b
union all
select * from TAB_B""",
        {"tab_b"},
        {"tab_a"},
    )


def test_parenthesis():
    assert_table_lineage_equal("(SELECT * FROM tab1)", {"tab1"}, None)


def test_create():
    assert_table_lineage_equal("CREATE TABLE tab1 (col1 STRING)", None, {"tab1"})


def test_create_if_not_exist():
    assert_table_lineage_equal(
        "CREATE TABLE IF NOT EXISTS tab1 (col1 STRING)", None, {"tab1"}
    )


def test_create_as():
    assert_table_lineage_equal(
        "CREATE TABLE tab1 AS SELECT * FROM tab2", {"tab2"}, {"tab1"}
    )


def test_create_as_with_parenthesis_around_select_statement():
    sql = "CREATE TABLE tab1 AS (SELECT * FROM tab2)"
    assert_table_lineage_equal(sql, {"tab2"}, {"tab1"})


def test_create_as_with_parenthesis_around_table_name():
    assert_table_lineage_equal(
        "CREATE TABLE tab1 AS SELECT * FROM (tab2)", {"tab2"}, {"tab1"}
    )


def test_create_as_with_parenthesis_around_both():
    sql = "CREATE TABLE tab1 AS (SELECT * FROM (tab2))"
    assert_table_lineage_equal(sql, {"tab2"}, {"tab1"})


def test_create_like():
    assert_table_lineage_equal("CREATE TABLE tab1 LIKE tab2", {"tab2"}, {"tab1"})


def test_create_view():
    assert_table_lineage_equal(
        """CREATE VIEW view1
as
SELECT
    col1,
    col2
FROM tab1
GROUP BY
col1""",
        {"tab1"},
        {"view1"},
    )


def test_merge_into_using_table():
    sql = """MERGE INTO target
USING src ON target.k = src.k
WHEN MATCHED THEN UPDATE SET target.v = src.v"""
    assert_table_lineage_equal(sql, {"src"}, {"target"})


def test_merge_into_using_subquery():
    sql = """MERGE INTO target USING (select k, max(v) as v from src group by k) AS b ON target.k = b.k
WHEN MATCHED THEN UPDATE SET target.v = b.v
WHEN NOT MATCHED THEN INSERT (k, v) VALUES (b.k, b.v)"""
    assert_table_lineage_equal(sql, {"src"}, {"target"})


def test_create_after_drop():
    assert_table_lineage_equal(
        "DROP TABLE IF EXISTS tab1; CREATE TABLE IF NOT EXISTS tab1 (col1 STRING)",
        None,
        {"tab1"},
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
insert into tab_c select * from tab_a;
drop table tab_a;"""
    assert_table_lineage_equal(sql, {"tab_b"}, {"tab_c"})


def test_new_create_tab_as_tmp_table():
    sql = """create table tab_a as select * from tab_b;
create table tab_c as select * from tab_a;"""
    assert_table_lineage_equal(sql, {"tab_b"}, {"tab_c"})


def test_alter_table_rename():
    assert_table_lineage_equal("alter table tab1 rename to tab2;", None, None)


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
        "insert into tab1 select * from tab2; alter table tab1 rename to tab3;",
        {"tab2"},
        {"tab3"},
    )


def test_truncate_table():
    assert_table_lineage_equal("truncate table tab1", None, None)


def test_delete_from_table():
    assert_table_lineage_equal("delete from table tab1", None, None)


def test_statements_trim_comment():
    comment = "------------------\n"
    sql = "select * from dual;"
    assert LineageRunner(comment + sql).statements()[0] == sql


def test_split_statements():
    sql = "SELECT * FROM tab1; SELECT * FROM tab2;"
    assert len(split(sql)) == 2


def test_split_statements_with_heading_and_ending_new_line():
    sql = "\nSELECT * FROM tab1;\nSELECT * FROM tab2;\n"
    assert len(split(sql)) == 2


def test_split_statements_with_comment():
    sql = """SELECT 1;

-- SELECT 2;"""
    assert len(split(sql)) == 1


def test_split_statements_with_show_create_table():
    sql = """SELECT 1;

SHOW CREATE TABLE tab1;"""
    assert len(split(sql)) == 2


def test_split_statements_with_desc():
    sql = """SELECT 1;

DESC tab1;"""
    assert len(split(sql)) == 2
