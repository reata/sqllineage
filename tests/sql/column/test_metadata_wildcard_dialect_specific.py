import pytest

from sqllineage.core.metadata_provider import MetaDataProvider
from sqllineage.utils.entities import ColumnQualifierTuple

from ...helpers import assert_column_lineage_equal

# Dialects that can leave columns out of a wildcard, and the keyword each one uses:
# https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#select_except
# https://clickhouse.com/docs/sql-reference/statements/select#except
# https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-select
# https://duckdb.org/docs/stable/sql/expressions/star#exclude-clause
# https://docs.aws.amazon.com/redshift/latest/dg/r_EXCLUDE_list.html
# https://docs.snowflake.com/en/sql-reference/sql/select#parameters
_wildcard_exclusion_syntax = [
    ("bigquery", "except"),
    ("clickhouse", "except"),
    ("databricks", "except"),
    ("sparksql", "except"),
    ("duckdb", "exclude"),
    ("redshift", "exclude"),
    ("snowflake", "exclude"),
]


@pytest.mark.parametrize("dialect, keyword", _wildcard_exclusion_syntax)
def test_select_wildcard_except_single_column(
    provider: MetaDataProvider, dialect: str, keyword: str
):
    sql = f"""insert into temp.creditcard_snapshot
select * {keyword} (creditcardid)
from marts.dim_credit_card"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("creditcard_key", "marts.dim_credit_card"),
                ColumnQualifierTuple("creditcard_key", "temp.creditcard_snapshot"),
            ),
            (
                ColumnQualifierTuple("cardtype", "marts.dim_credit_card"),
                ColumnQualifierTuple("cardtype", "temp.creditcard_snapshot"),
            ),
        ],
        dialect=dialect,
        metadata_provider=provider,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect, keyword", _wildcard_exclusion_syntax)
def test_select_wildcard_except_multiple_columns(
    provider: MetaDataProvider, dialect: str, keyword: str
):
    sql = f"""insert into temp.address_snapshot
select * {keyword} (addressid, state_name)
from marts.dim_address"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("address_key", "marts.dim_address"),
                ColumnQualifierTuple("address_key", "temp.address_snapshot"),
            ),
            (
                ColumnQualifierTuple("city_name", "marts.dim_address"),
                ColumnQualifierTuple("city_name", "temp.address_snapshot"),
            ),
            (
                ColumnQualifierTuple("country_name", "marts.dim_address"),
                ColumnQualifierTuple("country_name", "temp.address_snapshot"),
            ),
        ],
        dialect=dialect,
        metadata_provider=provider,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect, keyword", _wildcard_exclusion_syntax)
def test_select_qualified_wildcard_except_column(
    provider: MetaDataProvider, dialect: str, keyword: str
):
    sql = f"""insert into temp.creditcard_snapshot
select c.* {keyword} (creditcardid)
from marts.dim_credit_card c"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("creditcard_key", "marts.dim_credit_card"),
                ColumnQualifierTuple("creditcard_key", "temp.creditcard_snapshot"),
            ),
            (
                ColumnQualifierTuple("cardtype", "marts.dim_credit_card"),
                ColumnQualifierTuple("cardtype", "temp.creditcard_snapshot"),
            ),
        ],
        dialect=dialect,
        metadata_provider=provider,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect, keyword", _wildcard_exclusion_syntax)
def test_select_wildcard_except_column_from_subquery(dialect: str, keyword: str):
    """
    the excluded columns also apply when the wildcard is expanded from a subquery's columns
    rather than from metadata
    """
    sql = f"""insert into tab1
select * {keyword} (col2)
from (select col1, col2, col3 from tab2) sq"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("col1", "tab1"),
            ),
            (
                ColumnQualifierTuple("col3", "tab2"),
                ColumnQualifierTuple("col3", "tab1"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", ["duckdb", "redshift", "snowflake"])
def test_select_wildcard_exclude_single_column_without_parenthesis(
    provider: MetaDataProvider, dialect: str
):
    sql = """insert into temp.creditcard_snapshot
select * exclude creditcardid
from marts.dim_credit_card"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("creditcard_key", "marts.dim_credit_card"),
                ColumnQualifierTuple("creditcard_key", "temp.creditcard_snapshot"),
            ),
            (
                ColumnQualifierTuple("cardtype", "marts.dim_credit_card"),
                ColumnQualifierTuple("cardtype", "temp.creditcard_snapshot"),
            ),
        ],
        dialect=dialect,
        metadata_provider=provider,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", ["databricks", "sparksql"])
def test_select_wildcard_except_struct_field_keeps_column(dialect: str):
    """
    a multi-part name in EXCEPT removes a field from a struct column, the column itself is still selected
    """
    sql = """insert into tab1
select * except (col2.field1)
from (select col1, col2 from tab2) sq"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab2"),
                ColumnQualifierTuple("col1", "tab1"),
            ),
            (
                ColumnQualifierTuple("col2", "tab2"),
                ColumnQualifierTuple("col2", "tab1"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", ["bigquery", "snowflake"])
def test_select_wildcard_replace_keeps_all_columns(
    provider: MetaDataProvider, dialect: str
):
    """
    REPLACE changes the value of a column but, unlike EXCEPT, keeps it in the wildcard expansion
    """
    sql = """insert into temp.creditcard_snapshot
select * replace (upper(cardtype) as cardtype)
from marts.dim_credit_card"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("creditcard_key", "marts.dim_credit_card"),
                ColumnQualifierTuple("creditcard_key", "temp.creditcard_snapshot"),
            ),
            (
                ColumnQualifierTuple("creditcardid", "marts.dim_credit_card"),
                ColumnQualifierTuple("creditcardid", "temp.creditcard_snapshot"),
            ),
            (
                ColumnQualifierTuple("cardtype", "marts.dim_credit_card"),
                ColumnQualifierTuple("cardtype", "temp.creditcard_snapshot"),
            ),
        ],
        dialect=dialect,
        metadata_provider=provider,
        test_sqlparse=False,
    )


@pytest.mark.parametrize(
    "dialect, keyword", [("bigquery", "except"), ("snowflake", "exclude")]
)
def test_select_wildcard_except_and_replace(
    provider: MetaDataProvider, dialect: str, keyword: str
):
    sql = f"""insert into temp.creditcard_snapshot
select * {keyword} (creditcardid) replace (upper(cardtype) as cardtype)
from marts.dim_credit_card"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("creditcard_key", "marts.dim_credit_card"),
                ColumnQualifierTuple("creditcard_key", "temp.creditcard_snapshot"),
            ),
            (
                ColumnQualifierTuple("cardtype", "marts.dim_credit_card"),
                ColumnQualifierTuple("cardtype", "temp.creditcard_snapshot"),
            ),
        ],
        dialect=dialect,
        metadata_provider=provider,
        test_sqlparse=False,
    )
