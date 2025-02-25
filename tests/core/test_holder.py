from sqllineage.core.holders import StatementLineageHolder
from sqllineage.runner import LineageRunner


def test_dummy():
    assert str(StatementLineageHolder()) == repr(StatementLineageHolder())


def test_performance(benchmark):
    sql = """
CREATE TABLE database_b.table_b AS
  SELECT col_a,
         col_b,
         col_c,
         col_d,
         col_e,
         col_f,
         col_g,
         col_i,
         col_j,
         col_k
  FROM   database_a.table_a
    """
    runner = LineageRunner(sql=sql, dialect="ansi")
    runner.source_tables  # trigger SQL evaluation and graph generation
    benchmark(runner.get_column_lineage)
