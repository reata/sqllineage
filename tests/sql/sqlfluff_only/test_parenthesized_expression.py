"""
sqlparse implementation is capable of generating the correct result, however with redundant nodes in graph
"""

from sqllineage.utils.entities import ColumnQualifierTuple
from ...helpers import assert_column_lineage_equal, assert_table_lineage_equal


def test_subquery_expression_without_source_table():
    assert_column_lineage_equal(
        """INSERT INTO foo
SELECT (SELECT col1 + col2 AS result) AS sum_result
FROM bar""",
        [
            (
                ColumnQualifierTuple("col1", "bar"),
                ColumnQualifierTuple("sum_result", "foo"),
            ),
            (
                ColumnQualifierTuple("col2", "bar"),
                ColumnQualifierTuple("sum_result", "foo"),
            ),
        ],
        skip_graph_check=True,
    )


def test_insert_into_values_with_subquery():
    assert_table_lineage_equal(
        "INSERT INTO tab1 VALUES (1, (SELECT max(id) FROM tab2))",
        {"tab2"},
        {"tab1"},
        skip_graph_check=True,
    )


def test_insert_into_values_with_multiple_subquery():
    assert_table_lineage_equal(
        "INSERT INTO tab1 VALUES ((SELECT max(id) FROM tab2), (SELECT max(id) FROM tab3))",
        {"tab2", "tab3"},
        {"tab1"},
        skip_graph_check=True,
    )


def test_insert_into_values_with_multiple_subquery_in_multiple_row():
    assert_table_lineage_equal(
        "INSERT INTO tab1 VALUES (1, (SELECT max(id) FROM tab2)), (2, (SELECT max(id) FROM tab3))",
        {"tab2", "tab3"},
        {"tab1"},
        skip_graph_check=True,
    )
