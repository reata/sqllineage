import os
import tempfile

from sqllineage.cli import main
from sqllineage.core.models import SubQuery
from sqllineage.runner import LineageRunner
from sqllineage.utils.constant import LineageLevel

from ..helpers import assert_table_lineage_equal


def test_runner_dummy():
    runner = LineageRunner(
        """insert into tab2 select col1, col2, col3, col4, col5, col6 from tab1;
insert into tab3 select * from tab2""",
        verbose=True,
    )
    assert str(runner)
    assert runner.to_cytoscape() is not None
    assert runner.to_cytoscape(level=LineageLevel.COLUMN) is not None


def test_statements_trim_comment():
    comment = "------------------\n"
    sql = "select * from dual;"
    assert LineageRunner(comment + sql).statements()[0] == sql


def test_silent_mode():
    sql = "begin; select * from dual;"
    LineageRunner(sql, dialect="greenplum", silent_mode=True)._eval()


def test_get_column_lineage_exclude_subquery_inpath():
    v_sql = "insert into ta select b from (select b from tb union all select c from tc ) sub"
    parse = LineageRunner(sql=v_sql)
    for col_tuple in parse.get_column_lineage(exclude_subquery_columns=True):
        for col in col_tuple:
            assert not isinstance(col.parent, SubQuery)


def test_respect_sqlfluff_configuration_file():
    sqlfluff_config = """[sqlfluff:templater:jinja:context]
num_things=456
tbl_name=my_table"""
    cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tmpdirname:
        try:
            os.chdir(tmpdirname)
            with open(".sqlfluff", "w") as f:
                f.write(sqlfluff_config)
            sql = "SELECT {{ num_things }} FROM {{ tbl_name }} WHERE id > 10 LIMIT 5"
            assert_table_lineage_equal(sql, {"my_table"}, test_sqlparse=False)
        finally:
            os.chdir(cwd)


def test_respect_nested_sqlfluff_configuration_file():
    sqlfluff_config = """[sqlfluff:templater:jinja:context]
num_things=456
tbl_name=my_table"""
    cwd = os.getcwd()
    with tempfile.TemporaryDirectory() as tmpdirname:
        try:
            os.chdir(tmpdirname)
            nested_dir = os.path.join(tmpdirname, "nested_dir")
            os.mkdir(nested_dir)
            with open(os.path.join(nested_dir, ".sqlfluff"), "w") as f:
                f.write(sqlfluff_config)
            with open(os.path.join(nested_dir, "nested.sql"), "w") as f:
                f.write(
                    "SELECT {{ num_things }} FROM {{ tbl_name }} WHERE id > 10 LIMIT 5"
                )
            main(["-f", nested_dir + "/nested.sql"])
        finally:
            os.chdir(cwd)


def test_performance_comparison(benchmark_collection, benchmark):
    """This test simulates a rather complex SQL, spanning multiple tables and columns.

    NetworkX, the default way and package to generate all paths, is in this case rather
    slow. According to this benchmark and test, it takes at least twice the time than
    using rustworkX.

    The benchmark fixture only allows comparing best, mean/average, and worst. Better
    would be median due to large outliers.
    """

    def create_sql(num, sources=None) -> str:
        if sources:
            return ", ".join(
                [f"{s}.col_{i} AS col_{s}{i}" for i in range(num) for s in sources]
            )
        else:
            return ", ".join([f"col_{i} AS col_{i}" for i in range(num)])

    sql = ""

    for from_, to_ in [
        ("database_a.table_a", "database_b.table_a"),
        ("database_a.table_a", "database_b.table_b"),
        ("database_a.table_b", "database_b.table_c"),
        ("database_a.table_b", "database_b.table_d"),
        ("database_b.table_a", "database_c.table_a"),
        ("database_b.table_c", "database_c.table_b"),
    ]:
        sql += f"""
        CREATE TABLE {to_} AS
          SELECT {create_sql(10)}
          FROM {from_};
        """

    sql += f"""
    CREATE TABLE database_d.table_a AS
      SELECT {create_sql(10)}
      FROM database_a.table_a as x, database_b.table_b as y 
      WHERE x.col_0 = y.col_0;
    """

    sql += f"""
    CREATE TABLE database_d.table_b AS
      SELECT {create_sql(num=10, sources=["x", "y"])}
      FROM database_c.table_b as x, database_d.table_a as y 
      WHERE x.col_0 = y.col_0;
    """

    runner = LineageRunner(sql=sql, dialect="ansi")
    runner.graph  # trigger SQL evaluation and graph generation

    # the normal runtime for our test is pretty long. Updating settings to cater for it.
    benchmark_collection.config.ideal_rounds = 50
    benchmark_collection.config.max_time_ns = 6e10  # 60 sec

    nx = benchmark(runner.get_column_lineage, True, False, False, name="networkx")
    rx = benchmark(runner.get_column_lineage, True, False, True, name="rustworkx")
    assert (
        rx.best_ns * 2 < nx.best_ns
    ), f"rustworkx {rx.mean_ns} SHOULD BE at least 100% better than networkx {nx.mean_ns}"
