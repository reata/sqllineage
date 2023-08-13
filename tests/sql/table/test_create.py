from ...helpers import assert_table_lineage_equal


def test_create():
    assert_table_lineage_equal(
        "CREATE TABLE tab1 (col1 STRING)",
        None,
        {"tab1"},
        skip_graph_check=True,  # sqlfluff graph includes table to column edge
    )


def test_create_if_not_exist():
    assert_table_lineage_equal(
        "CREATE TABLE IF NOT EXISTS tab1 (col1 STRING)",
        None,
        {"tab1"},
        skip_graph_check=True,  # sqlfluff graph includes table to column edge
    )


def test_create_as():
    assert_table_lineage_equal(
        "CREATE TABLE tab1 AS SELECT * FROM tab2", {"tab2"}, {"tab1"}
    )


def test_create_like():
    assert_table_lineage_equal("CREATE TABLE tab1 LIKE tab2", {"tab2"}, {"tab1"})


def test_create_view():
    assert_table_lineage_equal(
        """CREATE VIEW view1
as
SELECT
    col1,
    col2
FROM tab1
GROUP BY
col1""",
        {"tab1"},
        {"view1"},
    )
