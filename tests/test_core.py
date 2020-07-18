from sqllineage.core import LineageResult


def test_dummy():
    assert str(LineageResult()) == repr(LineageResult())
    assert LineageResult() + LineageResult() is not None
