from .helpers import helper


def test_insert_into():
    helper("INSERT INTO tab1 VALUES (1, 2)", set(), {"tab1"})


def test_insert_into_with_keyword_table():
    helper("INSERT INTO TABLE tab1 VALUES (1, 2)", set(), {"tab1"})


def test_insert_into_with_columns():
    helper("INSERT INTO tab1 (col1, col2) SELECT * FROM tab2;", {"tab2"}, {"tab1"})


def test_insert_into_with_columns_and_select():
    helper("INSERT INTO tab1 (col1, col2) SELECT * FROM tab2", {"tab2"}, {"tab1"})


def test_insert_into_with_columns_and_select_union():
    helper("INSERT INTO tab1 (col1, col2) SELECT * FROM tab2 UNION SELECT * FROM tab3", {"tab2", "tab3"}, {"tab1"})


def test_insert_overwrite():
    helper("INSERT OVERWRITE TABLE tab1 SELECT col1 FROM tab2", {"tab2"}, {"tab1"})


def test_insert_overwrite_values():
    helper("INSERT OVERWRITE TABLE tab1 VALUES ('val1', 'val2'), ('val3', 'val4')", {}, {"tab1"})
