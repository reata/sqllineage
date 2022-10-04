import json
import pathlib
from unittest.mock import patch

import pytest

from sqllineage.cli import main


@patch("flask.Flask.run")
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


def test_e_and_j_args_exception():
    with pytest.raises(SystemExit) as e:
        main(["-g", "-j"])
    assert e.value.code == 1


def sorted_json(item):
    """
    Necessary to get working equality comparison between unsorted JSON objects.
    See here for source: https://www.geeksforgeeks.org/how-to-compare-json-objects-regardless-of-order-in-python/
    """
    if isinstance(item, dict):
        return sorted((key, sorted_json(values)) for key, values in item.items())
    if isinstance(item, list):
        return sorted(sorted_json(x) for x in item)
    else:
        return item


def test_nonjson_cli_output(capfd):
    """
    Test that table-level normal CLI lineage output is as expected.
    """
    main(["-e", "CREATE VIEW vw1 AS SELECT col1 FROM tab1"])  # Run a trivial lineage
    # The output we want
    want = """Statements(#): 1
Source Tables:
    <default>.tab1
Target Tables:
    <default>.vw1\n\n"""
    stdout, stderror = capfd.readouterr()  # Capture stdout, stderror
    # Test that the lineage output we have is the lineage we want
    assert stdout == want


def test_json_cli_output(capfd):
    """
    Test that table-level json CLI lineage output is as expected.
    """
    main(
        ["--json", "-e", "CREATE VIEW vw1 AS SELECT col1 FROM tab1"]
    )  # Run a trivial lineage
    # The output we want
    want = '{"dag": [{"data": {"id": "<default>.tab1"}}, {"data": {"id": "<default>.vw1"}}, {"data": {"id": "e0", "source": "<default>.tab1", "target": "<default>.vw1"}}]}\n'  # noqa: E501
    stdout, stderror = capfd.readouterr()  # Capture stdout, stderror
    # Test that the lineage output we have is the lineage we want
    assert sorted_json(json.loads(str(stdout))) == sorted_json(json.loads(want))


def test_nonjson_cli_column_output(capfd):
    """
    Test that column-level normal CLI lineage output is as expected.
    """
    main(
        ["--level=column", "-e", "CREATE VIEW vw1 AS SELECT col1 FROM tab1"]
    )  # Run a trivial lineage
    # The output we want
    want = "<default>.vw1.col1 <- <default>.tab1.col1\n"
    stdout, stderror = capfd.readouterr()  # Capture stdout, stderror
    # Test that the lineage output we have is the lineage we want
    assert stdout == want


def test_json_cli_column_output(capfd):
    """
    Test that column-level JSON CLI lineage output is as expected.
    """
    main(
        ["--level=column", "--json", "-e", "CREATE VIEW vw1 AS SELECT col1 FROM tab1"]
    )  # Run a trivial lineage
    # The output we want - yes we want a singleline string output this long
    want = '{"column": [{"data": {"id": "<default>.tab1.col1", "parent": "<default>.tab1", "parent_candidates": [{"name": "<default>.tab1", "type": "Table"}], "type": "Column"}}, {"data": {"id": "<default>.vw1.col1", "parent": "<default>.vw1", "parent_candidates": [{"name": "<default>.vw1", "type": "Table"}], "type": "Column"}}, {"data": {"id": "<default>.tab1", "type": "Table"}}, {"data": {"id": "<default>.vw1", "type": "Table"}}, {"data": {"id": "e0", "source": "<default>.tab1.col1", "target": "<default>.vw1.col1"}}], "dag": [{"data": {"id": "<default>.vw1"}}, {"data": {"id": "<default>.tab1"}}, {"data": {"id": "e0", "source": "<default>.tab1", "target": "<default>.vw1"}}]}\n'  # noqa: E501
    stdout, stderror = capfd.readouterr()  # Capture stdout, stderror
    # Test that the lineage output we have is the lineage we want
    assert sorted_json(json.loads(str(stdout))) == sorted_json(json.loads(want))
