import pytest

from sqllineage.runner import LineageRunner
from sqllineage.utils.entities import ColumnQualifierTuple

from ...helpers import assert_column_lineage_equal

# https://cwiki.apache.org/confluence/display/Hive/LanguageManual+LateralView
# https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-lateral-view.html
_lateral_view_dialects = ["databricks", "hive", "sparksql"]


@pytest.mark.parametrize("dialect", _lateral_view_dialects)
def test_select_column_from_lateral_view(dialect: str):
    sql = """INSERT INTO tab1
SELECT t.id, x.item
FROM tab2 t
LATERAL VIEW explode(t.items) x AS item"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("id", "tab2"),
                ColumnQualifierTuple("id", "tab1"),
            ),
            (
                ColumnQualifierTuple("items", "tab2"),
                ColumnQualifierTuple("item", "tab1"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", _lateral_view_dialects)
def test_select_unqualified_column_from_lateral_view(dialect: str):
    sql = """INSERT INTO tab1
SELECT id, item
FROM tab2
LATERAL VIEW OUTER explode(items) x AS item"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("id", "tab2"),
                ColumnQualifierTuple("id", "tab1"),
            ),
            (
                ColumnQualifierTuple("items", "tab2"),
                ColumnQualifierTuple("item", "tab1"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", _lateral_view_dialects)
def test_select_column_from_lateral_view_on_subquery(dialect: str):
    sql = """INSERT OVERWRITE TABLE tab1
SELECT a.id,
       get_json_object(b.item, '$.name') AS name
FROM (SELECT id, split(item_list, '#') AS items FROM tab2) a
LATERAL VIEW explode(a.items) b AS item"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("id", "tab2"),
                ColumnQualifierTuple("id", "tab1"),
            ),
            (
                ColumnQualifierTuple("item_list", "tab2"),
                ColumnQualifierTuple("name", "tab1"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", _lateral_view_dialects)
def test_select_column_from_chained_lateral_views(dialect: str):
    sql = """INSERT INTO tab1
SELECT t.id, y.pos, y.val, z.k, z.v
FROM tab2 t
LATERAL VIEW explode(t.nested) x AS arr
LATERAL VIEW posexplode(x.arr) y AS pos, val
LATERAL VIEW json_tuple(t.js, 'k', 'v') z AS k, v"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("id", "tab2"),
                ColumnQualifierTuple("id", "tab1"),
            ),
            (
                ColumnQualifierTuple("nested", "tab2"),
                ColumnQualifierTuple("pos", "tab1"),
            ),
            (
                ColumnQualifierTuple("nested", "tab2"),
                ColumnQualifierTuple("val", "tab1"),
            ),
            (
                ColumnQualifierTuple("js", "tab2"),
                ColumnQualifierTuple("k", "tab1"),
            ),
            (
                ColumnQualifierTuple("js", "tab2"),
                ColumnQualifierTuple("v", "tab1"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


# Hive accepts a lateral view without column aliases since 0.12.0, but the sqlfluff hive dialect follows the
# documented grammar "LATERAL VIEW udtf(expression) tableAlias AS columnAlias", so hive is not tested here
@pytest.mark.parametrize("dialect", ["databricks", "sparksql"])
def test_select_column_from_lateral_view_without_column_alias(dialect: str):
    sql = """INSERT INTO tab1
SELECT x.col
FROM tab2
LATERAL VIEW explode(items) x"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("items", "tab2"),
                ColumnQualifierTuple("col", "tab1"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", _lateral_view_dialects)
def test_select_wildcard_from_lateral_view(dialect: str):
    sql = """INSERT INTO tab1
SELECT *
FROM tab2
LATERAL VIEW explode(items) x AS item"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("*", "tab2"),
                ColumnQualifierTuple("*", "tab1"),
            ),
            (
                ColumnQualifierTuple("items", "tab2"),
                ColumnQualifierTuple("item", "tab1"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", _lateral_view_dialects)
def test_select_column_from_lateral_view_in_union(dialect: str):
    sql = """INSERT INTO tab1
SELECT t.id, x.item
FROM tab2 t
LATERAL VIEW explode(t.items) x AS item
UNION ALL
SELECT t.id, x.item
FROM tab3 t
LATERAL VIEW explode(t.elements) x AS item"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("id", "tab2"),
                ColumnQualifierTuple("id", "tab1"),
            ),
            (
                ColumnQualifierTuple("items", "tab2"),
                ColumnQualifierTuple("item", "tab1"),
            ),
            (
                ColumnQualifierTuple("id", "tab3"),
                ColumnQualifierTuple("id", "tab1"),
            ),
            (
                ColumnQualifierTuple("elements", "tab3"),
                ColumnQualifierTuple("item", "tab1"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", _lateral_view_dialects)
def test_select_count_wildcard_from_lateral_view(dialect: str):
    # count(*) reads every column but writes none of the lateral view columns
    sql = """INSERT INTO tab1
SELECT count(*) AS cnt
FROM tab2
LATERAL VIEW explode(items) x AS item"""
    lr = LineageRunner(sql, dialect=dialect)
    assert {str(path[-1]) for path in lr.get_column_lineage()} == {"<default>.tab1.cnt"}
