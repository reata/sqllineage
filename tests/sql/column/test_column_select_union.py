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


def test_union_where_later_branch_introduces_new_output_columns():
    # When a later UNION branch has more columns than the first, write_columns grows lazily
    # during the column loop as add_column_lineage calls accumulate HAS_COLUMN edges.
    # The fix re-evaluates holder.write_columns inside the loop so later columns in the
    # same barrier see the updated list and map to the correct output column by position.
    #
    # With the buggy pre-hoisted check the length snapshot was taken before the loop, so
    # it saw 1 element vs col_grp of 2 and fell back to tgt_col_from_query for every idx.
    # That mapped t2.a -> tgt.a (wrong) instead of t2.a -> tgt.b (the position-1 output).
    sql = """INSERT INTO tgt
SELECT a FROM t1
UNION ALL
SELECT b, a FROM t2"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("a", "t1"),
                ColumnQualifierTuple("a", "tgt"),
            ),
            (
                ColumnQualifierTuple("b", "t2"),
                ColumnQualifierTuple("b", "tgt"),
            ),
            (
                ColumnQualifierTuple("a", "t2"),
                ColumnQualifierTuple("b", "tgt"),
            ),
        ],
        test_sqlparse=False,
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
