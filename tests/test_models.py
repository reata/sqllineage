import pytest

from sqllineage.exceptions import SQLLineageException
from sqllineage.models import Schema, Table


def test_repr_dummy():
    assert repr(Schema())
    assert repr(Table(""))
    with pytest.raises(SQLLineageException):
        Table("a.b.c")
    with pytest.warns(Warning):
        Table("a.b", Schema("c"))


def test_hash_eq():
    assert Schema("a") == Schema("a")
    assert len({Schema("a"), Schema("a")}) == 1
    assert Table("a") == Table("a")
    assert len({Table("a"), Table("a")}) == 1
