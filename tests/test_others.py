from sqllineage.core import LineageParser
from .helpers import helper


def test_use():
    helper("USE db1")


def test_split_statements():
    sql = "SELECT * FROM tab1; SELECT * FROM tab2;"
    assert len(LineageParser(sql).statements) == 2
