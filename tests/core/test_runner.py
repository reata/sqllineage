import os
import tempfile

import pytest

from sqllineage.cli import main
from sqllineage.config import SQLLineageConfig
from sqllineage.core.models import Column, SubQuery, Table
from sqllineage.exceptions import AmbiguousNode
from sqllineage.runner import LineageRunner
from sqllineage.utils.constant import LineageLevel

from ..helpers import _gen_graph_operators, assert_table_lineage_equal


def test_runner_dummy():
    for graph_operator in _gen_graph_operators():
        with SQLLineageConfig(GRAPH_OPERATOR_CLASS=graph_operator):
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


# two columns per table, so "tab2.col1" is distinct from "tab2.col2"
_MULTI_COL_SQL = "insert into tab2 select col1, col2 from tab1; insert into tab3 select col1 from tab2"
# one column per table, so a bare table name resolves to exactly one column
_SINGLE_COL_SQL = (
    "insert into tab2 select col1 from tab1; insert into tab3 select col1 from tab2"
)


def _naive_filter(all_paths, node_str):
    """reference implementation: compute everything, keep paths touching node_str"""

    def _matches(col):
        return (
            col.raw_name == node_str
            or str(col) == node_str
            or str(col).endswith("." + node_str)
        )

    return {path for path in all_paths if any(_matches(col) for col in path)}


def test_node_filter_matches_naive_filtering():
    for graph_operator in _gen_graph_operators():
        with SQLLineageConfig(GRAPH_OPERATOR_CLASS=graph_operator):
            full = set(LineageRunner(_MULTI_COL_SQL).get_column_lineage())
            for node_str in ["tab2.col1", "tab2.col2", "tab1.col2", "tab3.col1"]:
                filtered = set(
                    LineageRunner(_MULTI_COL_SQL).get_column_lineage(node=node_str)
                )
                assert filtered == _naive_filter(full, node_str), node_str


def test_node_filter_by_string_column():
    lr = LineageRunner(_MULTI_COL_SQL)
    result = lr.get_column_lineage(node="tab2.col1")
    assert len(result) > 0
    assert all(any(str(col).endswith(".tab2.col1") for col in path) for path in result)


def test_node_filter_by_table_object():
    lr = LineageRunner(_SINGLE_COL_SQL)
    result = lr.get_column_lineage(node=Table("tab2"))
    assert len(result) > 0
    assert all(any(col.parent == Table("tab2") for col in path) for path in result)


def test_node_filter_by_bare_table_name():
    lr = LineageRunner(_SINGLE_COL_SQL)
    result = lr.get_column_lineage(node="tab2")
    assert len(result) > 0
    assert all(any(col.parent == Table("tab2") for col in path) for path in result)


def test_node_filter_multi_column_table_is_ambiguous():
    lr = LineageRunner(_MULTI_COL_SQL)
    with pytest.raises(AmbiguousNode) as exc_info:
        lr.get_column_lineage(node="tab2")
    assert len(exc_info.value.matches) == 2


def test_node_filter_no_match_returns_empty():
    lr = LineageRunner(_SINGLE_COL_SQL)
    assert lr.get_column_lineage(node="does_not_exist") == []


def test_node_filter_ambiguous_raises():
    sql = (
        "insert into tab2 select col1 from tab1; insert into tab3 select col1 from tab1"
    )
    lr = LineageRunner(sql)
    with pytest.raises(AmbiguousNode) as exc_info:
        lr.get_column_lineage(node="col1")
    assert len(exc_info.value.matches) > 1


def test_node_filter_none_unchanged():
    lr1 = LineageRunner(_MULTI_COL_SQL)
    lr2 = LineageRunner(_MULTI_COL_SQL)
    assert lr1.get_column_lineage() == lr2.get_column_lineage(node=None)


def test_node_filter_by_column_object():
    lr = LineageRunner(_MULTI_COL_SQL)
    lr._eval()
    col = Column("col1")
    col.parent = Table("tab2")
    result = lr.get_column_lineage(node=col)
    assert len(result) > 0
    assert all(col in path for path in result)


def test_node_filter_cone_with_no_reachable_target_returns_empty():
    # sub.c is selected from tb but never projected into ta, so it has no
    # path to a real table target once subquery-ending paths are pruned
    sql = "insert into ta select b from (select b, c from tb) sub"
    lr = LineageRunner(sql)
    assert lr.get_column_lineage(node="tb.c") == []


def test_node_filter_exclude_subquery_columns_drops_collapsed_path():
    # sub.b has no upstream column (literal value), so once the subquery
    # column is stripped, the remaining path is a single column and dropped
    sql = "insert into ta select b from (select 1 as b) sub"
    lr = LineageRunner(sql)
    assert lr.get_column_lineage(node="ta.b", exclude_subquery_columns=True) == []


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
