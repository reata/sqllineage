import pytest

from sqllineage.core.metadata_provider import MetaDataProvider
from sqllineage.utils.entities import ColumnQualifierTuple
from ....helpers import assert_column_lineage_equal, generate_metadata_providers


providers = generate_metadata_providers(
    {
        "db1.table1": ["id", "a", "b", "c", "d"],
        "db2.table2": ["id", "h", "i", "j", "k"],
        "db3.table3": ["pk", "p", "q", "r"],
    }
)


@pytest.mark.parametrize("provider", providers)
def test_do_not_register_session_metadata_for_update_statement(
    provider: MetaDataProvider,
):
    sql = """UPDATE db1.table1 SET a = 1;

CREATE TABLE db1.foo AS
SELECT a, b, c
FROM db1.table1 tab1
INNER JOIN db1.table2 tab2 ON tab1.id = tab2.id
"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("a", "db1.table1"),
                ColumnQualifierTuple("a", "db1.foo"),
            ),
            (
                ColumnQualifierTuple("b", "db1.table1"),
                ColumnQualifierTuple("b", "db1.foo"),
            ),
            (
                ColumnQualifierTuple("c", "db1.table1"),
                ColumnQualifierTuple("c", "db1.foo"),
            ),
        ],
        metadata_provider=provider,
    )
