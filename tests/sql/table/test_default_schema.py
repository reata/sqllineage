from unittest.mock import patch

from sqllineage.utils.entities import ColumnQualifierTuple
from tests.helpers import assert_column_lineage_equal

meta_collect = {
    "ods.source_a": ["day_id", "user_id", "user_name"],
    "ods.target_tab": ["day_no", "user_code", "name"],
}


@patch(
    "os.environ",
    {
        "SQLLINEAGE_DEFAULT_SCHEMA": "ods",
    },
)
def test_metadata_target_column():
    sql = """insert into target_tab select user_name,day_id,user_id from source_a"""
    assert_column_lineage_equal(
        sql=sql,
        column_lineages=[
            (
                ColumnQualifierTuple("user_name", "ods.source_a"),
                ColumnQualifierTuple("user_name", "ods.target_tab"),
            ),
            (
                ColumnQualifierTuple("day_id", "ods.source_a"),
                ColumnQualifierTuple("day_id", "ods.target_tab"),
            ),
            (
                ColumnQualifierTuple("user_id", "ods.source_a"),
                ColumnQualifierTuple("user_id", "ods.target_tab"),
            ),
        ],
    )