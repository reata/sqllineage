from sqllineage.models import Database, Table


def test_repr_dummy():
    assert repr(Database())
    assert repr(Table(""))


def test_hash_eq():
    assert Database("a") == Database("a")
    assert len({Database("a"), Database("a")}) == 1
    assert Table("a") == Table("a")
    assert len({Table("a"), Table("a")}) == 1
