import pytest

from sqllineage.utils.entities import ColumnQualifierTuple

from ...helpers import assert_column_lineage_equal


@pytest.mark.parametrize("dialect", ["databricks", "sparksql"])
def test_column_lineage_with_column_type_definitions(dialect: str):
    """
    CTAS with column type definitions should produce clean column names in lineage.

    sqlfluff's column_definition parse tree differs by dialect:
      ANSI, bigquery, postgres, ... : column_definition → identifier
      sparksql, databricks          : column_definition → column_reference → identifier
    """
    sql = """CREATE TABLE target_tbl(
   col_a string,
   col_b string)
AS
SELECT col1 AS col_a,
       col2 AS col_b
FROM source_tbl"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "source_tbl"),
                ColumnQualifierTuple("col_a", "target_tbl"),
            ),
            (
                ColumnQualifierTuple("col2", "source_tbl"),
                ColumnQualifierTuple("col_b", "target_tbl"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )
