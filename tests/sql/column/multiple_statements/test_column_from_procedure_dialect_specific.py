import pytest

from sqllineage.utils.entities import ColumnQualifierTuple

from ....helpers import assert_column_lineage_equal


@pytest.mark.parametrize("dialect", ["bigquery"])
def test_lineage_column_from_procedure(dialect: str):
    sql = """CREATE PROCEDURE `test-lineage-bq`.dataset_a.sp_test (
  param_a INT64, param_b DATE
)
BEGIN
  DECLARE var_a INT64 DEFAULT 0;
  IF param_a IS NULL THEN
    SET param_a = 0;
  ELSEIF param_a < 0 THEN
    SET param_a = ABS(param_a);
  END IF;
  SET param_b = COALESCE(param_b, CURRENT_DATE('Asia/Shanghai'));
  LOOP
    SET var_a = var_a + 1;
    INSERT INTO `test-lineage-bq.dataset_a.table_c`
    SELECT
      a.col1 AS id_pk,
      a.col2 AS updated,
      b.col2 AS pet
    FROM test-lineage-bq.dataset_a.table_a AS a
    LEFT JOIN test-lineage-bq.dataset_a.table_b AS b
      ON a.col1 = b.col1
    WHERE DATE(a.col2) >= param_b;
    SELECT @@ROW_COUNT AS aff_rows;
    IF var_a >= param_a THEN LEAVE;
    END IF;
  END LOOP;
EXCEPTION WHEN ERROR THEN
  SELECT @@ERROR.MESSAGE AS error_info;
  RAISE;
END;"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "test-lineage-bq.dataset_a.table_a"),
                ColumnQualifierTuple("id_pk", "test-lineage-bq.dataset_a.table_c"),
            ),
            (
                ColumnQualifierTuple("col2", "test-lineage-bq.dataset_a.table_a"),
                ColumnQualifierTuple("updated", "test-lineage-bq.dataset_a.table_c"),
            ),
            (
                ColumnQualifierTuple("col2", "test-lineage-bq.dataset_a.table_b"),
                ColumnQualifierTuple("pet", "test-lineage-bq.dataset_a.table_c"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )
