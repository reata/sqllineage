import pytest

from sqllineage.core.metadata_provider import MetaDataProvider
from sqllineage.utils.entities import ColumnQualifierTuple

from ....conftest import data_warehouse_schemas
from ....helpers import assert_column_lineage_equal, generate_metadata_providers

providers = generate_metadata_providers(data_warehouse_schemas)


@pytest.mark.parametrize("provider", providers)
def test_do_not_register_session_metadata_for_update_statement(
    provider: MetaDataProvider,
):
    sql = """UPDATE sales.salesorderheader SET status = 5 WHERE salesorderid = 1001;

CREATE TABLE temp.order_line_extract AS
SELECT salesorderdetailid, productid, unitprice, orderqty
FROM sales.salesorderdetail sod
INNER JOIN sales.salesorderheader soh ON sod.salesorderid = soh.salesorderid 
"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("salesorderdetailid", "sales.salesorderdetail"),
                ColumnQualifierTuple("salesorderdetailid", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("productid", "sales.salesorderdetail"),
                ColumnQualifierTuple("productid", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("unitprice", "sales.salesorderdetail"),
                ColumnQualifierTuple("unitprice", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("orderqty", "sales.salesorderdetail"),
                ColumnQualifierTuple("orderqty", "temp.order_line_extract"),
            ),
        ],
        metadata_provider=provider,
    )
