from sqllineage.core import DataSetLineage


def test_dummy():
    assert str(DataSetLineage()) == repr(DataSetLineage())
    assert DataSetLineage() + DataSetLineage() is not None
