import pytest

from sqllineage.core.metadata_provider import MetaDataProvider
from sqllineage.utils.entities import ColumnQualifierTuple
from ...helpers import assert_column_lineage_equal, generate_metadata_providers


providers = generate_metadata_providers(
    {
        "ods.source_tab": ["day_id", "user_id", "name"],
        "ods.target_tab": ["day_no", "user_code", "user_name"],
    }
)


@pytest.mark.parametrize("provider", providers)
def test_metadata_target_column(provider: MetaDataProvider):
    sql = """insert into ods.target_tab select day_id as acct_id, user_id as xxx, name as yyy from ods.source_tab"""
    assert_column_lineage_equal(
        sql=sql,
        column_lineages=[
            (
                ColumnQualifierTuple("name", "ods.source_tab"),
                ColumnQualifierTuple("user_name", "ods.target_tab"),
            ),
            (
                ColumnQualifierTuple("day_id", "ods.source_tab"),
                ColumnQualifierTuple("day_no", "ods.target_tab"),
            ),
            (
                ColumnQualifierTuple("user_id", "ods.source_tab"),
                ColumnQualifierTuple("user_code", "ods.target_tab"),
            ),
        ],
        metadata_provider=provider,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("provider", providers)
def test_metadata_target_column_cte(provider: MetaDataProvider):
    sql = """
INSERT INTO ods.target_tab
WITH cte_table AS (SELECT day_id as acct_id, user_id as xxx, name as yyy FROM ods.source_tab)
SELECT acct_id, xxx, yyy FROM cte_table"""
    assert_column_lineage_equal(
        sql=sql,
        column_lineages=[
            (
                ColumnQualifierTuple("user_id", "ods.source_tab"),
                ColumnQualifierTuple("user_code", "ods.target_tab"),
            ),
            (
                ColumnQualifierTuple("day_id", "ods.source_tab"),
                ColumnQualifierTuple("day_no", "ods.target_tab"),
            ),
            (
                ColumnQualifierTuple("name", "ods.source_tab"),
                ColumnQualifierTuple("user_name", "ods.target_tab"),
            ),
        ],
        metadata_provider=provider,
        test_sqlparse=False,
    )
