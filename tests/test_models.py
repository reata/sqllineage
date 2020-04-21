from sqllineage.models import Schema, Table


def test_repr_dummy():
    assert repr(Schema())
    assert repr(Table(""))


def test_hash_eq():
    assert Schema("a") == Schema("a")
    assert len({Schema("a"), Schema("a")}) == 1
    assert Table("a") == Table("a")
    assert len({Table("a"), Table("a")}) == 1
