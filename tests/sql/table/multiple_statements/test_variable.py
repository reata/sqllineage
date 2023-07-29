import pytest

from ....helpers import assert_table_lineage_equal


@pytest.mark.parametrize("dialect", ["tsql"])
def test_tsql_declare(dialect: str):
    """
    https://learn.microsoft.com/en-us/sql/t-sql/language-elements/declare-local-variable-transact-sql?view=sql-server-ver16
    """
    sql = """DECLARE @age int = 10;
INSERT INTO tgt
SELECT Name,@age
FROM People;"""
    assert_table_lineage_equal(sql, {"People"}, {"tgt"}, dialect=dialect)
