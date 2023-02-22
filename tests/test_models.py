import pytest
from sqlparse.sql import Parenthesis

from sqllineage.core.exceptions import SQLLineageException
from sqllineage.core.models import Column, Path, Schema, SubQuery, Table
from sqllineage.core.parser.sqlparse.models import (
    SqlParseColumn,
    SqlParseSubQuery,
    SqlParseTable,
)


def test_repr_dummy():
    assert repr(Schema())
    assert repr(SqlParseTable(""))
    assert repr(SqlParseTable("a.b.c"))
    assert repr(SqlParseSubQuery(Parenthesis(), Parenthesis().value, ""))
    assert repr(SqlParseColumn("a.b"))
    assert repr(Path(""))
    with pytest.raises(SQLLineageException):
        SqlParseTable("a.b.c.d")
    with pytest.warns(Warning):
        SqlParseTable("a.b", Schema("c"))


def test_hash_eq():
    assert Schema("a") == Schema("a")
    assert len({Schema("a"), Schema("a")}) == 1
    assert SqlParseTable("a") == SqlParseTable("a")
    assert len({SqlParseTable("a"), SqlParseTable("a")}) == 1


def test_of_dummy():
    with pytest.raises(NotImplementedError):
        Column.of("")
    with pytest.raises(NotImplementedError):
        Table.of("")
    with pytest.raises(NotImplementedError):
        SubQuery.of("", None)
