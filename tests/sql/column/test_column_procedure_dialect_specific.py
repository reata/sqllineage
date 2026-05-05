import pytest

from sqllineage.utils.entities import ColumnQualifierTuple

from ...helpers import assert_column_lineage_equal

proc_dialects = [
    "bigquery",
    "oracle",
    "tsql",
]


@pytest.mark.parametrize("dialect", proc_dialects)
def test_procedure_column(dialect: str):
    sql = f"""CREATE PROCEDURE db1.proc1()
{'' if dialect == 'bigquery' else 'AS'}
BEGIN
INSERT INTO tab2 (col1)
SELECT col1 FROM tab1;
END"""
    assert_column_lineage_equal(
        sql,
        [(ColumnQualifierTuple("col1", "tab1"), ColumnQualifierTuple("col1", "tab2"))],
        dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", proc_dialects)
def test_procedure_column_multiple_statements(dialect: str):
    sql = f"""CREATE PROCEDURE proc1()
{'' if dialect == 'bigquery' else 'AS'}
BEGIN
INSERT INTO tab2 (col1)
SELECT col1 FROM tab1;
INSERT INTO tab3 (col2)
SELECT col2 FROM tab2;
END"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab1"),
                ColumnQualifierTuple("col1", "tab2"),
            ),
            (
                ColumnQualifierTuple("col2", "tab2"),
                ColumnQualifierTuple("col2", "tab3"),
            ),
        ],
        dialect,
        test_sqlparse=False,
    )


def test_procedure_column_bigquery():
    sql = """CREATE PROCEDURE `test-lineage-bq`.dataset_a.sp_test(
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
        dialect="bigquery",
        test_sqlparse=False,
    )


def test_procedure_column_oracle():
    sql = """CREATE PROCEDURE schema_a.sp_test(
  param_a NUMBER, param_b DATE
) AS
DECLARE var_a NUMBER := 0;
BEGIN
  IF param_a IS NULL THEN
    param_a := 0;
  ELSIF param_a < 0 THEN
    param_a := ABS(param_a);
  END IF;
  param_b := COALESCE(param_b, CURRENT_DATE('Asia/Shanghai'));
  LOOP
    var_a := var_a + 1;
    INSERT INTO schema_a.table_c
    SELECT
      a.col1 AS id_pk,
      a.col2 AS updated,
      b.col2 AS pet
    FROM schema_a.table_a AS a
    LEFT JOIN schema_a.table_b AS b
      ON a.col1 = b.col1
    WHERE DATE(a.col2) >= param_b;
    SELECT SQL%ROWCOUNT AS aff_rows;
    EXIT WHEN var_a >= param_a;
  END LOOP;
EXCEPTION WHEN ERROR THEN
  SELECT SQLERRM AS error_info;
  RAISE;
END;
/"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "schema_a.table_a"),
                ColumnQualifierTuple("id_pk", "schema_a.table_c"),
            ),
            (
                ColumnQualifierTuple("col2", "schema_a.table_a"),
                ColumnQualifierTuple("updated", "schema_a.table_c"),
            ),
            (
                ColumnQualifierTuple("col2", "schema_a.table_b"),
                ColumnQualifierTuple("pet", "schema_a.table_c"),
            ),
        ],
        dialect="oracle",
        test_sqlparse=False,
    )


def test_procedure_column_tsql():
    sql = """CREATE PROCEDURE schema_a.sproc_test
  @param_a BIGINT, @param_b DATE
AS
BEGIN
  SET NOCOUNT ON;
  BEGIN TRY
    DECLARE @var_a BIGINT = 0;
    IF @param_a IS NULL
      SET @param_a = 0;
    ELSE IF @param_a < 0
      SET @param_a = ABS(@param_a);
    SET
      @param_b = COALESCE(
        @param_b,
        CAST(SYSDATETIMEOFFSET() AT TIME ZONE 'China Standard Time' AS DATE)
      );
    WHILE @var_a >= @param_a
      BEGIN
        SET @var_a = @var_a + 1;
        INSERT INTO schema_a.table_c
        SELECT
          a.col1 AS id_pk,
          a.col2 AS updated,
          b.col2 AS pet
        FROM schema_a.table_a AS a
        LEFT JOIN schema_a.table_b AS b
          ON a.col1 = b.col1
        WHERE DATE(a.col2) >= @param_b;
        SELECT @@ROWCOUNT AS aff_rows;
      END
  END TRY
  BEGIN CATCH
    SELECT ERROR_MESSAGE() AS error_info;
    THROW;
  END CATCH
END;
GO"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "schema_a.table_a"),
                ColumnQualifierTuple("id_pk", "schema_a.table_c"),
            ),
            (
                ColumnQualifierTuple("col2", "schema_a.table_a"),
                ColumnQualifierTuple("updated", "schema_a.table_c"),
            ),
            (
                ColumnQualifierTuple("col2", "schema_a.table_b"),
                ColumnQualifierTuple("pet", "schema_a.table_c"),
            ),
        ],
        dialect="tsql",
        test_sqlparse=False,
    )
