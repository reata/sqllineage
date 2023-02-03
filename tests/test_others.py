from sqllineage.core.models import Path
from sqllineage.runner import LineageRunner
from sqllineage.sqlfluff_core.models import SqlFluffPath
from .helpers import assert_table_lineage_equal


def test_use():
    assert_table_lineage_equal("USE db1")


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


def test_create_after_drop():
    assert_table_lineage_equal(
        "DROP TABLE IF EXISTS tab1; CREATE TABLE IF NOT EXISTS tab1 (col1 STRING)",
        None,
        {"tab1"},
    )


# deactivated for sqlfluff since it can not be parsed properly
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
        test_sqlfluff=False,
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


# the previous query "COPY tab1 FROM tab2" was wrong
# Reference:
# https://www.postgresql.org/docs/current/sql-copy.html (Postgres)
# https://docs.aws.amazon.com/es_es/redshift/latest/dg/r_COPY.html (Redshift)
def test_copy_from_table():
    assert_table_lineage_equal(
        "COPY tab1 FROM 's3://mybucket/mypath'",
        {Path("s3://mybucket/mypath")},
        {"tab1"},
        test_sqlfluff=False,
    )
    assert_table_lineage_equal(
        "COPY tab1 FROM 's3://mybucket/mypath'",
        {SqlFluffPath("s3://mybucket/mypath")},
        {"tab1"},
        "redshift",
        test_sqlparse=False,
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


def test_truncate_table():
    assert_table_lineage_equal("truncate table tab1", None, None)


def test_delete_from_table():
    assert_table_lineage_equal("delete from table tab1", None, None)


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
