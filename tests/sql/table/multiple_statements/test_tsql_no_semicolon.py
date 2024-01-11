import pytest

from ....helpers import assert_table_lineage_equal


@pytest.mark.parametrize("dialect", ["tsql"])
def test_tsql_multi_statement_no_semicolon(dialect: str):
    """
    tsql multiple statements without explicit semicolon as splitter.
    """
    sql = """insert into tab1 select * from foo
insert into tab2 select * from bar"""
    assert_table_lineage_equal(
        sql,
        {"foo", "bar"},
        {"tab1", "tab2"},
        dialect=dialect,
        test_sqlparse=False,
        tsql_no_semicolon=True,
    )
