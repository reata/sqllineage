from sqllineage.utils.entities import ColumnQualifierTuple

from ...helpers import assert_column_lineage_equal


def test_column_reference_using_union():
    sql = """INSERT INTO tab3
SELECT col1
FROM tab1
UNION ALL
SELECT col1
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab1"),
                ColumnQualifierTuple("col1", "tab3"),
            ),
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("col1", "tab3"),
            ),
        ],
    )
    sql = """INSERT INTO tab3
SELECT col1
FROM tab1
UNION
SELECT col1
FROM tab2"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab1"),
                ColumnQualifierTuple("col1", "tab3"),
            ),
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("col1", "tab3"),
            ),
        ],
    )


def test_union_inside_cte():
    sql = """INSERT INTO dataset.target WITH temp_cte AS (SELECT col1 FROM dataset.tab1 UNION ALL
SELECT col1 FROM dataset.tab2) SELECT col1 FROM temp_cte"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "dataset.tab1"),
                ColumnQualifierTuple("col1", "dataset.target"),
            ),
            (
                ColumnQualifierTuple("col1", "dataset.tab2"),
                ColumnQualifierTuple("col1", "dataset.target"),
            ),
        ],
    )


def test_union_with_subquery():
    sql = """INSERT INTO tab3
SELECT sq1.id
FROM (SELECT id
      FROM tab1) sq1
UNION ALL
SELECT sq2.id
FROM (SELECT id
      FROM tab2) sq2"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("id", "tab1"),
                ColumnQualifierTuple("id", "tab3"),
            ),
            (
                ColumnQualifierTuple("id", "tab2"),
                ColumnQualifierTuple("id", "tab3"),
            ),
        ],
    )
