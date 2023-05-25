from sqllineage.utils.entities import ColumnQualifierTuple
from .helpers import assert_column_lineage_equal, assert_table_lineage_equal


def test_insert_into():
    assert_table_lineage_equal("INSERT INTO tab1 VALUES (1, 2)", set(), {"tab1"})


def test_insert_into_with_columns():
    assert_table_lineage_equal(
        "INSERT INTO tab1 (col1, col2) SELECT * FROM tab2;",
        {"tab2"},
        {"tab1"},
        skip_graph_validation=True,
    )


def test_insert_into_with_columns_and_select():
    assert_table_lineage_equal(
        "INSERT INTO tab1 (col1, col2) SELECT * FROM tab2",
        {"tab2"},
        {"tab1"},
        skip_graph_validation=True,
    )


def test_insert_into_with_columns_and_select_union():
    assert_table_lineage_equal(
        "INSERT INTO tab1 (col1, col2) SELECT * FROM tab2 UNION SELECT * FROM tab3",
        {"tab2", "tab3"},
        {"tab1"},
        skip_graph_validation=True,
    )


def test_insert_with_custom_columns():
    # test with query as subquery
    sql = "insert into trg_tbl(random1,random2) (select col1,col2 from src_tbl)"
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "src_tbl"),
                ColumnQualifierTuple("random1", "trg_tbl"),
            ),
            (
                ColumnQualifierTuple("col2", "src_tbl"),
                ColumnQualifierTuple("random2", "trg_tbl"),
            ),
        ],
        test_sqlparse=False,
    )

    # test with plain query
    sql = "insert into trg_tbl(random1,random2) select col1,col2 from src_tbl_new"
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "src_tbl_new"),
                ColumnQualifierTuple("random1", "trg_tbl"),
            ),
            (
                ColumnQualifierTuple("col2", "src_tbl_new"),
                ColumnQualifierTuple("random2", "trg_tbl"),
            ),
        ],
        test_sqlparse=False,
    )
