from sqllineage.core.metadata_provider import MetaDataProvider
from sqllineage.utils.entities import ColumnQualifierTuple

from ...helpers import assert_column_lineage_equal


def test_metadata_target_column(provider: MetaDataProvider):
    sql = """insert into marts.dim_date
select
    hash(date_day) as date_key,
    date_day as dd,
    prior_date_day as prd,
    next_date_day as ndd,
    prior_year_date_day as pydd,
    prior_year_over_year_date_day as pyoydd,
    day_of_week as dow,
    day_of_week_name as down,
    day_of_month as dom,
    day_of_year as doy
from date.date"""
    assert_column_lineage_equal(
        sql=sql,
        column_lineages=[
            (
                ColumnQualifierTuple("date_day", "date.date"),
                ColumnQualifierTuple("date_key", "marts.dim_date"),
            ),
            (
                ColumnQualifierTuple("date_day", "date.date"),
                ColumnQualifierTuple("date_day", "marts.dim_date"),
            ),
            (
                ColumnQualifierTuple("prior_date_day", "date.date"),
                ColumnQualifierTuple("prior_date_day", "marts.dim_date"),
            ),
            (
                ColumnQualifierTuple("next_date_day", "date.date"),
                ColumnQualifierTuple("next_date_day", "marts.dim_date"),
            ),
            (
                ColumnQualifierTuple("prior_year_date_day", "date.date"),
                ColumnQualifierTuple("prior_year_date_day", "marts.dim_date"),
            ),
            (
                ColumnQualifierTuple("prior_year_over_year_date_day", "date.date"),
                ColumnQualifierTuple("prior_year_over_year_date_day", "marts.dim_date"),
            ),
            (
                ColumnQualifierTuple("day_of_week", "date.date"),
                ColumnQualifierTuple("day_of_week", "marts.dim_date"),
            ),
            (
                ColumnQualifierTuple("day_of_week_name", "date.date"),
                ColumnQualifierTuple("day_of_week_name", "marts.dim_date"),
            ),
            (
                ColumnQualifierTuple("day_of_month", "date.date"),
                ColumnQualifierTuple("day_of_month", "marts.dim_date"),
            ),
            (
                ColumnQualifierTuple("day_of_year", "date.date"),
                ColumnQualifierTuple("day_of_year", "marts.dim_date"),
            ),
        ],
        metadata_provider=provider,
        test_sqlparse=False,
    )


def test_metadata_target_column_cte(provider: MetaDataProvider):
    sql = """
INSERT INTO marts.dim_date
WITH cte_table AS (select
    hash(date_day) as date_key,
    date_day as dd,
    prior_date_day as prd,
    next_date_day as ndd,
    prior_year_date_day as pydd,
    prior_year_over_year_date_day as pyoydd,
    day_of_week as dow,
    day_of_week_name as down,
    day_of_month as dom,
    day_of_year as doy
from date.date)
SELECT date_key, dd, prd, ndd, pydd, pyoydd, dow, down, dom, doy FROM cte_table"""
    assert_column_lineage_equal(
        sql=sql,
        column_lineages=[
            (
                ColumnQualifierTuple("date_day", "date.date"),
                ColumnQualifierTuple("date_key", "marts.dim_date"),
            ),
            (
                ColumnQualifierTuple("date_day", "date.date"),
                ColumnQualifierTuple("date_day", "marts.dim_date"),
            ),
            (
                ColumnQualifierTuple("prior_date_day", "date.date"),
                ColumnQualifierTuple("prior_date_day", "marts.dim_date"),
            ),
            (
                ColumnQualifierTuple("next_date_day", "date.date"),
                ColumnQualifierTuple("next_date_day", "marts.dim_date"),
            ),
            (
                ColumnQualifierTuple("prior_year_date_day", "date.date"),
                ColumnQualifierTuple("prior_year_date_day", "marts.dim_date"),
            ),
            (
                ColumnQualifierTuple("prior_year_over_year_date_day", "date.date"),
                ColumnQualifierTuple("prior_year_over_year_date_day", "marts.dim_date"),
            ),
            (
                ColumnQualifierTuple("day_of_week", "date.date"),
                ColumnQualifierTuple("day_of_week", "marts.dim_date"),
            ),
            (
                ColumnQualifierTuple("day_of_week_name", "date.date"),
                ColumnQualifierTuple("day_of_week_name", "marts.dim_date"),
            ),
            (
                ColumnQualifierTuple("day_of_month", "date.date"),
                ColumnQualifierTuple("day_of_month", "marts.dim_date"),
            ),
            (
                ColumnQualifierTuple("day_of_year", "date.date"),
                ColumnQualifierTuple("day_of_year", "marts.dim_date"),
            ),
        ],
        metadata_provider=provider,
        test_sqlparse=False,
    )
