import pytest

from sqllineage.combiners import LineageCombiner, NaiveLineageCombiner


def test_dummy():
    assert NaiveLineageCombiner.combine()
    with pytest.raises(NotImplementedError):
        LineageCombiner.combine()
