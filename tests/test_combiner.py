from sqllineage.combiners import NaiveLineageCombiner


def test_dummy():
    assert NaiveLineageCombiner.combine()
