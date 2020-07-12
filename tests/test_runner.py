import pytest

from sqllineage.runner import LineageRunner, main


def test_dummy():
    assert str(LineageRunner("select * from tab", verbose=True))
    with pytest.raises(SystemExit) as e:
        main()
    assert e.value.code == 2
