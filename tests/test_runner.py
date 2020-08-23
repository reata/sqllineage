import pathlib
from unittest.mock import patch

import pytest

from sqllineage.runner import LineageRunner, main


@patch("matplotlib.pyplot.show")
def test_dummy(_):
    assert str(
        LineageRunner(
            """insert into tab2 select col1, col2, col3, col4, col5, col6 from tab1;
insert overwrite table tab3 select * from tab2""",
            verbose=True,
        )
    )
    main([])
    main(["-e", "select * from dual"])
    main(["-f", __file__])
    main(["-e", "select * from dual", "-f", __file__])
    main(
        [
            "-e",
            "insert overwrite table tab1 select * from tab1 union select * from tab2",
            "-g",
        ]
    )


def test_file_exception():
    for args in (["-f", str(pathlib.Path().absolute())], ["-f", "nonexist_file"]):
        with pytest.raises(SystemExit) as e:
            main(args)
        assert e.value.code == 1
