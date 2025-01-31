import pytest

from sqllineage.core.metadata_provider import MetaDataProvider
from sqllineage.utils.entities import ColumnQualifierTuple
from ...helpers import assert_column_lineage_equal, generate_metadata_providers


providers = generate_metadata_providers(
    {
        "database_a.table_a": ["col_a", "col_b", "col_c"],
    }
)


@pytest.mark.parametrize("provider", providers)
def test_ouput_consistency(provider: MetaDataProvider):
    sql = """CREATE TABLE database_b.table_c
    AS (
      SELECT
        *,
        1 AS event_time
      FROM (
        SELECT
          table_b.col_b AS col_a
        FROM database_b.table_b AS table_b
        JOIN database_a.table_a AS table_d
      ) AS base
    )
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col_b", "database_a.table_a"),
                ColumnQualifierTuple("col_b", "database_b.table_c"),
            ),
            (
                ColumnQualifierTuple("col_c", "database_a.table_a"),
                ColumnQualifierTuple("col_c", "database_b.table_c"),
            ),
            (
                ColumnQualifierTuple("col_b", "database_b.table_b"),
                ColumnQualifierTuple("col_a", "database_b.table_c"),
            ),
        ],
        dialect="athena",
        test_sqlparse=False,
        test_sqlfluff=True,
        metadata_provider=provider,
    )
