import pytest
from sqlparse.sql import Parenthesis

from sqllineage.core.models import Column, Path, Schema, SubQuery, Table
from sqllineage.exceptions import SQLLineageException


def test_repr_dummy():
    assert repr(Schema())
    assert repr(Table(""))
    assert repr(Table("a.b.c"))
    assert repr(SubQuery(Parenthesis(), Parenthesis().value, ""))
    assert repr(Column("a.b"))
    assert repr(Path(""))
    with pytest.raises(SQLLineageException):
        Table("a.b.c.d")
    with pytest.warns(Warning):
        Table("a.b", Schema("c"))


def test_hash_eq():
    assert Schema("a") == Schema("a")
    assert len({Schema("a"), Schema("a")}) == 1
    assert Table("a") == Table("a")
    assert len({Table("a"), Table("a")}) == 1


def test_of_dummy():
    with pytest.raises(NotImplementedError):
        Column.of("")
    with pytest.raises(NotImplementedError):
        Table.of("")
    with pytest.raises(NotImplementedError):
        SubQuery.of("", None)
