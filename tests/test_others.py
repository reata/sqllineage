from sqllineage.core import LineageParser
from .helpers import helper


def test_use():
    helper("USE db1")


def test_create():
    helper("CREATE TABLE tab1 (col1 STRING)", None, {"tab1"})


def test_create_if_not_exist():
    helper("CREATE TABLE IF NOT EXISTS tab1 (col1 STRING)", None, {"tab1"})


def test_create_as():
    helper("CREATE TABLE tab1 AS SELECT * FROM tab2", {"tab2"}, {"tab1"})


def test_create_like():
    helper("CREATE TABLE tab1 LIKE tab2", {"tab2"}, {"tab1"})


def test_create_after_drop():
    helper("DROP TABLE IF EXISTS tab1; CREATE TABLE IF NOT EXISTS tab1 (col1 STRING)", None, {"tab1"})


def test_drop():
    helper("DROP TABLE IF EXISTS tab1", None, None)


def test_drop_with_comment():
    helper("""--comment
DROP TABLE IF EXISTS tab1""", None, None)


def test_drop_after_create():
    helper("CREATE TABLE IF NOT EXISTS tab1 (col1 STRING);DROP TABLE IF EXISTS tab1", None, None)


def test_drop_tmp_tab_after_create():
    sql = """create table tab_a as select * from tab_b;
insert overwrite table tab_c select * from tab_a;
drop table tab_a;"""
    helper(sql, {"tab_b"}, {"tab_c"})


def test_create_select():
    helper("CREATE TABLE tab1 SELECT * FROM tab2", {"tab2"}, {"tab1"})


def test_split_statements():
    sql = "SELECT * FROM tab1; SELECT * FROM tab2;"
    assert len(LineageParser(sql).statements) == 2


def test_split_statements_with_heading_and_ending_new_line():
    sql = "\nSELECT * FROM tab1;\nSELECT * FROM tab2;\n"
    assert len(LineageParser(sql).statements) == 2


def test_split_statements_with_comment():
    sql = """SELECT 1;

-- SELECT 2;"""
    assert len(LineageParser(sql).statements) == 1


def test_split_statements_with_show_create_table():
    sql = """SELECT 1;

SHOW CREATE TABLE tab1;"""
    assert len(LineageParser(sql).statements) == 2


def test_split_statements_with_desc():
    sql = """SELECT 1;

DESC tab1;"""
    assert len(LineageParser(sql).statements) == 2
