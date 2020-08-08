import pathlib

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
    main([])
    main(["-e", "select * from dual"])
    main(["-f", __file__])
    main(["-e", "select * from dual", "-f", __file__])


def test_combiner_exception():
    with pytest.raises(ValueError):
        main(["-c", "malformatcombiner"])
    with pytest.raises(ImportError):
        main(["-c", "nonexist_package.nonexist_combiner"])
    with pytest.raises(AttributeError):
        main(["-c", "sqllineage.combiners.nonexist_combiner"])


def test_file_exception():
    with pytest.raises(IsADirectoryError):
        main(["-f", str(pathlib.Path().absolute())])
    with pytest.raises(FileNotFoundError):
        main(["-f", "nonexist_file"])
