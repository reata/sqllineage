from sqllineage.models import Database, Table


def test_repr_dummy():
    assert repr(Database())
    assert repr(Table(""))
