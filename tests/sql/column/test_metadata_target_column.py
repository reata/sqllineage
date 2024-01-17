from typing import Dict, List, Optional

from sqllineage.core.metadata_provider import MetaDataProvider
from sqllineage.utils.entities import ColumnQualifierTuple
from ...helpers import assert_column_lineage_equal


class MetaCollect(MetaDataProvider):
    def __init__(self, metadata: Optional[Dict[str, List[str]]]):
        super().__init__()
        self.metadata = metadata if metadata else {}

    def _get_table_columns(self, schema: str, table: str, **kwargs) -> List[str]:
        return self.metadata.get(f"{schema}.{table}", [])


meta_collect = {
    "ods.source_a": ["day_id", "user_id", "user_name"],
    "ods.target_tab": ["day_no", "user_code", "name"],
}


def test_metadata_target_column():
    sql = """insert into ods.target_tab select day_id as acct_id, user_id as xxx, user_name as yyy from ods.source_a"""
    assert_column_lineage_equal(
        sql=sql,
        column_lineages=[
            (
                ColumnQualifierTuple("user_name", "ods.source_a"),
                ColumnQualifierTuple("name", "ods.target_tab"),
            ),
            (
                ColumnQualifierTuple("day_id", "ods.source_a"),
                ColumnQualifierTuple("day_no", "ods.target_tab"),
            ),
            (
                ColumnQualifierTuple("user_id", "ods.source_a"),
                ColumnQualifierTuple("user_code", "ods.target_tab"),
            ),
        ],
        metadata_provider=MetaCollect(meta_collect),
        test_sqlparse=False,
    )


def test_metadata_target_column_cte():
    sql = """
INSERT INTO ods.target_tab
WITH cte1 AS (SELECT day_id as acct_id, user_id as xxx, user_name as yyy FROM ods.source_a)
SELECT acct_id, xxx, yyy FROM cte1"""
    assert_column_lineage_equal(
        sql=sql,
        column_lineages=[
            (
                ColumnQualifierTuple("user_id", "ods.source_a"),
                ColumnQualifierTuple("user_code", "ods.target_tab"),
            ),
            (
                ColumnQualifierTuple("day_id", "ods.source_a"),
                ColumnQualifierTuple("day_no", "ods.target_tab"),
            ),
            (
                ColumnQualifierTuple("user_name", "ods.source_a"),
                ColumnQualifierTuple("name", "ods.target_tab"),
            ),
        ],
        metadata_provider=MetaCollect(meta_collect),
        test_sqlparse=False,
    )


def test_metadata_target_column_cte2():
    sql = """INSERT INTO ods.target_tab
    (WITH tab1 AS (SELECT day_id as acct_id, user_id as xxx, user_name as yyy FROM ods.source_a)
    SELECT acct_id, xxx, yyy FROM tab1)"""
    assert_column_lineage_equal(
        sql=sql,
        column_lineages=[
            (
                ColumnQualifierTuple("user_name", "ods.source_a"),
                ColumnQualifierTuple("name", "ods.target_tab"),
            ),
            (
                ColumnQualifierTuple("day_id", "ods.source_a"),
                ColumnQualifierTuple("day_no", "ods.target_tab"),
            ),
            (
                ColumnQualifierTuple("user_id", "ods.source_a"),
                ColumnQualifierTuple("user_code", "ods.target_tab"),
            ),
        ],
        metadata_provider=MetaCollect(meta_collect),
        test_sqlparse=False,
    )


def test_metadata_target_column_cte3():
    sql = """ INSERT INTO ods.target_tab
    (WITH tab1 AS (SELECT day_id as acct_id, user_id as xxx, user_name as yyy FROM ods.source_a)
    SELECT acct_id, xxx, yyy FROM tab1);"""
    assert_column_lineage_equal(
        sql=sql,
        column_lineages=[
            (
                ColumnQualifierTuple("user_name", "ods.source_a"),
                ColumnQualifierTuple("name", "ods.target_tab"),
            ),
            (
                ColumnQualifierTuple("day_id", "ods.source_a"),
                ColumnQualifierTuple("day_no", "ods.target_tab"),
            ),
            (
                ColumnQualifierTuple("user_id", "ods.source_a"),
                ColumnQualifierTuple("user_code", "ods.target_tab"),
            ),
        ],
        metadata_provider=MetaCollect(meta_collect),
        test_sqlparse=False,
    )
