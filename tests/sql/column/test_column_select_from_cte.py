from sqllineage.utils.entities import ColumnQualifierTuple
from ...helpers import assert_column_lineage_equal


def test_column_reference_from_cte_using_qualifier():
    sql = """WITH wtab1 AS (SELECT col1 FROM tab2)
INSERT INTO tab1
SELECT wtab1.col1 FROM wtab1"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("col1", "tab2"), ColumnQualifierTuple("col1", "tab1"))],
    )


def test_column_reference_from_cte_using_alias():
    sql = """WITH wtab1 AS (SELECT col1 FROM tab2)
INSERT INTO tab1
SELECT wt.col1 FROM wtab1 wt"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("col1", "tab2"), ColumnQualifierTuple("col1", "tab1"))],
    )


def test_column_reference_from_previous_defined_cte():
    sql = """WITH
cte1 AS (SELECT a FROM tab1),
cte2 AS (SELECT a FROM cte1)
INSERT INTO tab2
SELECT a FROM cte2"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("a", "tab1"), ColumnQualifierTuple("a", "tab2"))],
    )


def test_multiple_column_references_from_previous_defined_cte():
    sql = """WITH
cte1 AS (SELECT a, b FROM tab1),
cte2 AS (SELECT a, max(b) AS b_max, count(b) AS b_cnt FROM cte1 GROUP BY a)
INSERT INTO tab2
SELECT cte1.a, cte2.b_max, cte2.b_cnt FROM cte1 JOIN cte2
WHERE cte1.a = cte2.a"""
    assert_column_lineage_equal(
        sql,
        [
            (ColumnQualifierTuple("a", "tab1"), ColumnQualifierTuple("a", "tab2")),
            (ColumnQualifierTuple("b", "tab1"), ColumnQualifierTuple("b_max", "tab2")),
            (ColumnQualifierTuple("b", "tab1"), ColumnQualifierTuple("b_cnt", "tab2")),
        ],
    )


def test_smarter_column_resolution_using_query_context():
    sql = """WITH
cte1 AS (SELECT a, b FROM tab1),
cte2 AS (SELECT c, d FROM tab2)
INSERT INTO tab3
SELECT b, d FROM cte1 JOIN cte2
WHERE cte1.a = cte2.c"""
    assert_column_lineage_equal(
        sql,
        [
            (ColumnQualifierTuple("b", "tab1"), ColumnQualifierTuple("b", "tab3")),
            (ColumnQualifierTuple("d", "tab2"), ColumnQualifierTuple("d", "tab3")),
        ],
    )
