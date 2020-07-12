import pytest

from sqllineage.runner import LineageRunner, main


def test_dummy():
    assert str(
        LineageRunner(
            "select col1, col2, col3, col4, col5, col6 from tab1;", verbose=True
        )
    )
    with pytest.raises(SystemExit) as e:
        main()
    assert e.value.code == 2
