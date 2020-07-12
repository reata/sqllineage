from sqllineage.core import LineageAnalyzer, LineageResult


def test_dummy():
    assert str(LineageResult())
    assert LineageResult() + LineageResult() is not None
