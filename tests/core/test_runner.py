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
