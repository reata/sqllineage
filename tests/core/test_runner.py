from tests.helpers import assert_table_lineage_equal

from sqllineage.runner import LineageRunner
from sqllineage.utils.constant import LineageLevel


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


def test_default_schema():
    sql = """insert into target_tab select user_id, user_name from source_tab_1, source_tab_2"""
    assert_table_lineage_equal(
        sql=sql,
        source_tables={"ods.source_tab_1", "ods.source_tab_2"},
        target_tables={"ods.target_tab"},
        default_schema="ods",
    )
