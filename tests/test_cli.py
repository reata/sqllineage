import pathlib
from unittest.mock import patch

import pytest

from sqllineage.cli import main


@patch("socketserver.BaseServer.serve_forever")
def test_cli_dummy(_):
    main([])
    main(["-e", "select * from dual"])
    main(["-f", __file__])
    main(["-e", "insert into foo select * from dual", "-l", "column"])
    main(["-e", "select * from dual", "-f", __file__])
    main(["-f", __file__, "-g"])
    main(["-g"])
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


@patch("builtins.open", side_effect=PermissionError())
def test_file_permission_error(_):
    with pytest.raises(SystemExit) as e:
        main(["-f", __file__])
    assert e.value.code == 1
