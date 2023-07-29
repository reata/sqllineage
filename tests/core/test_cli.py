import os
from pathlib import Path
from unittest.mock import patch

import pytest

from sqllineage import DATA_FOLDER
from sqllineage.cli import main


@patch("socketserver.BaseServer.serve_forever")
def test_cli_dummy(_):
    main([])
    main(["-e", "select * from dual"])
    main(["-e", "insert into foo select * from dual", "-l", "column"])
    for dirname, _, files in os.walk(DATA_FOLDER):
        if len(files) > 0:
            sql_file = str(Path(dirname).joinpath(Path(files[0])))
            main(["-f", sql_file])
            main(["-e", "select * from dual", "-f", sql_file])
            main(["-f", sql_file, "-g"])
            break
    main(["-g"])
    main(["-ds"])
    main(
        [
            "-e",
            "insert overwrite table tab1 select * from tab1 union select * from tab2",
            "-g",
        ]
    )


def test_file_exception():
    for args in (["-f", str(Path().absolute())], ["-f", "nonexist_file"]):
        with pytest.raises(SystemExit) as e:
            main(args)
        assert e.value.code == 1


@patch("builtins.open", side_effect=PermissionError())
def test_file_permission_error(_):
    with pytest.raises(SystemExit) as e:
        main(["-f", __file__])
    assert e.value.code == 1
