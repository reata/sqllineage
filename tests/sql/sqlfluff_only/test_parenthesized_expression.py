from sqllineage.utils.entities import ColumnQualifierTuple
from ...helpers import assert_column_lineage_equal


def test_subquery_expression_without_source_table():
    """
    sqlparse implementation is capable of generating the correct result, however with redundant nodes in graph
    """
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
        test_sqlparse=False,
    )
