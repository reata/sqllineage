import pytest

from ...helpers import assert_table_lineage_equal


@pytest.mark.parametrize("dialect", ["bigquery"])
def test_merge_without_into(dialect: str):
    """
    INTO is optional in BigQuery MERGE statement:
    https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement
    """
    sql = """MERGE target USING src AS b ON target.k = b.k
WHEN MATCHED THEN UPDATE SET target.v = b.v
WHEN NOT MATCHED THEN INSERT (k, v) VALUES (b.k, b.v)"""
    assert_table_lineage_equal(sql, {"src"}, {"target"}, dialect=dialect)


@pytest.mark.parametrize("dialect", ["bigquery"])
def test_merge_insert_row(dialect: str):
    """
    MERGE INSERT CLAUSE in BigQuery can be INSERT ROW without specifying columns via INSERT VALUES (col, ...)
    https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#merge_statement
    """
    sql = """MERGE INTO tgt t
USING src s
ON t.date = s.date and t.channel = s.channel
WHEN NOT MATCHED THEN
INSERT ROW
WHEN MATCHED THEN
UPDATE SET t.col = s.col"""
    assert_table_lineage_equal(
        sql,
        {"src"},
        {"tgt"},
        dialect=dialect,
    )
