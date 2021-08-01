from sqllineage.core.lineage_result import LineageResult


def test_dummy():
    assert str(LineageResult()) == repr(LineageResult())
