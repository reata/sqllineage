import pytest

from sqllineage.runner import LineageRunner, main


def test_dummy():
    assert str(
        LineageRunner(
            """insert into tab2 select col1, col2, col3, col4, col5, col6 from tab1;
insert overwrite table tab3 select * from tab2""",
            verbose=True,
        )
    )
    with pytest.raises(SystemExit) as e:
        main()
    assert e.value.code == 2
