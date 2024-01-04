from sqllineage.utils.entities import ColumnQualifierTuple
from tests.helpers import assert_table_lineage_equal


def test_default_schema():
    sql = """insert into target_tab  select  user_id, user_name from source_tab"""
    assert_table_lineage_equal(
        sql=sql,
        source_tables={'ods.source_tab'},
        target_tables={'ods.target_tab'},
        default_schema='ods',
        test_sqlparse=False)
