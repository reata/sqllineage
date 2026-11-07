import pytest

from sqllineage.utils.entities import ColumnQualifierTuple

from ...helpers import assert_column_lineage_equal

# https://prestodb.io/docs/current/sql/select.html#unnest
# https://trino.io/docs/current/sql/select.html#unnest
# https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-TABLEFUNCTIONS
_unnest_dialects = ["athena", "duckdb", "postgres", "trino"]


@pytest.mark.parametrize("dialect", _unnest_dialects)
def test_select_column_from_unnest(dialect: str):
    sql = """INSERT INTO tab1
SELECT s.id, t.item
FROM tab2 s
CROSS JOIN UNNEST(s.items) AS t (item)"""
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


@pytest.mark.parametrize("dialect", _unnest_dialects)
def test_select_unqualified_column_from_unnest(dialect: str):
    sql = """INSERT INTO tab1
SELECT student, score
FROM tests
CROSS JOIN UNNEST(scores) AS t (score)"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("student", "tests"),
                ColumnQualifierTuple("student", "tab1"),
            ),
            (
                ColumnQualifierTuple("scores", "tests"),
                ColumnQualifierTuple("score", "tab1"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", _unnest_dialects)
def test_select_column_from_unnest_without_as(dialect: str):
    sql = """INSERT INTO tab1
SELECT s.id, t.item
FROM tab2 s
CROSS JOIN UNNEST(s.items) t(item)"""
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


@pytest.mark.parametrize("dialect", _unnest_dialects)
def test_select_multiple_columns_from_unnest(dialect: str):
    sql = """INSERT INTO tab1
SELECT r.transaction_id, i.item_name, i.value
FROM tab2 AS r
CROSS JOIN UNNEST(r.execution_items) AS i (item_name, value)"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("transaction_id", "tab2"),
                ColumnQualifierTuple("transaction_id", "tab1"),
            ),
            (
                ColumnQualifierTuple("execution_items", "tab2"),
                ColumnQualifierTuple("item_name", "tab1"),
            ),
            (
                ColumnQualifierTuple("execution_items", "tab2"),
                ColumnQualifierTuple("value", "tab1"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", ["athena", "postgres", "trino"])
def test_select_column_from_unnest_with_ordinality(dialect: str):
    sql = """INSERT INTO tab1
SELECT t.n, t.a
FROM tab2
CROSS JOIN UNNEST(numbers) WITH ORDINALITY AS t (n, a)"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("numbers", "tab2"),
                ColumnQualifierTuple("n", "tab1"),
            ),
            (
                ColumnQualifierTuple("numbers", "tab2"),
                ColumnQualifierTuple("a", "tab1"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", _unnest_dialects)
def test_select_column_from_unnest_in_comma_join(dialect: str):
    sql = """INSERT INTO tab1
SELECT s.id, t.item
FROM tab2 s, UNNEST(s.items) AS t (item)"""
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


@pytest.mark.parametrize("dialect", _unnest_dialects)
def test_select_column_from_unnest_followed_by_join(dialect: str):
    sql = """INSERT INTO tab1
SELECT s.id, t.item, u.name
FROM tab2 s
CROSS JOIN UNNEST(s.items) AS t (item)
JOIN tab3 u ON u.id = s.id"""
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
                ColumnQualifierTuple("name", "tab3"),
                ColumnQualifierTuple("name", "tab1"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", _unnest_dialects)
def test_select_column_from_unnest_on_subquery(dialect: str):
    sql = """INSERT INTO tab1
SELECT a.id, t.item
FROM (SELECT id, split(item_list, ',') AS items FROM tab2) a
CROSS JOIN UNNEST(a.items) AS t (item)"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("id", "tab2"),
                ColumnQualifierTuple("id", "tab1"),
            ),
            (
                ColumnQualifierTuple("item_list", "tab2"),
                ColumnQualifierTuple("item", "tab1"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", _unnest_dialects)
def test_select_wildcard_from_unnest(dialect: str):
    sql = """INSERT INTO tab1
SELECT *
FROM tab2 s
CROSS JOIN UNNEST(s.items) AS t (item)"""
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


# https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#unnest_operator
@pytest.mark.parametrize("dialect", ["bigquery"])
def test_select_struct_field_from_unnest_without_column_alias(dialect: str):
    sql = """INSERT INTO tab1
SELECT s.id, c.name
FROM tab2 s, UNNEST(s.items) AS c"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("id", "tab2"),
                ColumnQualifierTuple("id", "tab1"),
            ),
            (
                ColumnQualifierTuple("items", "tab2"),
                ColumnQualifierTuple("name", "tab1"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", ["bigquery", "postgres"])
def test_select_alias_column_from_unnest_without_column_alias(dialect: str):
    # the alias names the single generated column when no column alias is specified
    sql = """INSERT INTO tab1
SELECT s.id, tag
FROM tab2 s
CROSS JOIN UNNEST(s.tags) AS tag"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("id", "tab2"),
                ColumnQualifierTuple("id", "tab1"),
            ),
            (
                ColumnQualifierTuple("tags", "tab2"),
                ColumnQualifierTuple("tag", "tab1"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


# https://docs.snowflake.com/en/sql-reference/functions/flatten
@pytest.mark.parametrize("dialect", ["snowflake"])
def test_select_column_from_lateral_flatten(dialect: str):
    sql = """INSERT INTO tab1
SELECT s.id, f.value::string AS item
FROM tab2 s, LATERAL FLATTEN(input => s.items) f"""
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


@pytest.mark.parametrize("dialect", ["snowflake"])
def test_select_column_from_table_flatten(dialect: str):
    sql = """INSERT INTO tab1
SELECT s.id, f.value:name::string AS name, f.index AS idx
FROM tab2 s, TABLE(FLATTEN(input => s.payload:items)) f"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("id", "tab2"),
                ColumnQualifierTuple("id", "tab1"),
            ),
            (
                ColumnQualifierTuple("payload", "tab2"),
                ColumnQualifierTuple("name", "tab1"),
            ),
            (
                ColumnQualifierTuple("payload", "tab2"),
                ColumnQualifierTuple("idx", "tab1"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )
