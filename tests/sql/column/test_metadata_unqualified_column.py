import pytest

from sqllineage.core.metadata_provider import MetaDataProvider
from sqllineage.utils.entities import ColumnQualifierTuple

from ...conftest import data_warehouse_schemas
from ...helpers import assert_column_lineage_equal, generate_metadata_providers

providers = generate_metadata_providers(data_warehouse_schemas)


@pytest.mark.parametrize("provider", providers)
def test_select_column_from_tables(provider: MetaDataProvider):
    sql = """insert into temp.order_line_extract
select t1.salesorderid, customerid as customer_id, territoryid, orderqty, unitprice as unit_price
from sales.salesorderdetail t1
left join sales.salesorderheader t2 on t1.salesorderid = t2.salesorderid
"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("salesorderid", "sales.salesorderdetail"),
                ColumnQualifierTuple("salesorderid", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("customerid", "sales.salesorderheader"),
                ColumnQualifierTuple("customer_id", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("territoryid", "sales.salesorderheader"),
                ColumnQualifierTuple("territoryid", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("orderqty", "sales.salesorderdetail"),
                ColumnQualifierTuple("orderqty", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("unitprice", "sales.salesorderdetail"),
                ColumnQualifierTuple("unit_price", "temp.order_line_extract"),
            ),
        ],
        metadata_provider=provider,
    )

    sql = """insert into temp.order_line_extract
select customerid, territoryid as territory, orderqty, unitprice as unit_price, weight, color, name as product_name
from sales.salesorderdetail t1
left join sales.salesorderheader t2 on t1.salesorderid = t2.salesorderid
left join production.product t3 on t2.salesorderid = t3.productid
"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("customerid", "sales.salesorderheader"),
                ColumnQualifierTuple("customerid", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("territoryid", "sales.salesorderheader"),
                ColumnQualifierTuple("territory", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("orderqty", "sales.salesorderdetail"),
                ColumnQualifierTuple("orderqty", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("unitprice", "sales.salesorderdetail"),
                ColumnQualifierTuple("unit_price", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("weight", "production.product"),
                ColumnQualifierTuple("weight", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("color", "production.product"),
                ColumnQualifierTuple("color", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("name", "production.product"),
                ColumnQualifierTuple("product_name", "temp.order_line_extract"),
            ),
        ],
        metadata_provider=provider,
    )


@pytest.mark.parametrize("provider", providers)
def test_select_column_from_subqueries(provider: MetaDataProvider):
    sql = """insert into temp.order_line_extract
select customerid, territoryid as territory, orderqty, detail_id
from (select orderqty, unitprice, salesorderdetailid as detail_id from sales.salesorderdetail) t1
left join (select customerid, territoryid, subtotal from sales.salesorderheader) t2
on t1.salesorderid = t2.salesorderid
"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("customerid", "sales.salesorderheader"),
                ColumnQualifierTuple("customerid", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("territoryid", "sales.salesorderheader"),
                ColumnQualifierTuple("territory", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("orderqty", "sales.salesorderdetail"),
                ColumnQualifierTuple("orderqty", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("salesorderdetailid", "sales.salesorderdetail"),
                ColumnQualifierTuple("detail_id", "temp.order_line_extract"),
            ),
        ],
        metadata_provider=provider,
    )

    sql = """insert into temp.order_line_extract
select orderqty, unitprice as unit_price, subtotal, territory, productid as product_id, color
from (select salesorderid, orderqty, unitprice, product from sales.salesorderdetail) t1
join (select salesorderid, subtotal, territoryid as territory, j from sales.salesorderheader) t2 on t1.salesorderid = t2.salesorderid
left join (select productid, color, weight from production.product) t3 on t1.product = t3.productid
"""  # noqa: E501
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("orderqty", "sales.salesorderdetail"),
                ColumnQualifierTuple("orderqty", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("unitprice", "sales.salesorderdetail"),
                ColumnQualifierTuple("unit_price", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("subtotal", "sales.salesorderheader"),
                ColumnQualifierTuple("subtotal", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("territoryid", "sales.salesorderheader"),
                ColumnQualifierTuple("territory", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("productid", "production.product"),
                ColumnQualifierTuple("product_id", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("color", "production.product"),
                ColumnQualifierTuple("color", "temp.order_line_extract"),
            ),
        ],
        metadata_provider=provider,
    )


@pytest.mark.parametrize("provider", providers)
def test_select_column_from_table_subquery(provider: MetaDataProvider):
    sql = """insert into temp.order_line_extract
select unitprice as unit_price, orderqty, weight, name as product_name
from sales.salesorderdetail t1
left join (select name, color, weight from production.product) t3
on t1.productid = t3.productid
"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("unitprice", "sales.salesorderdetail"),
                ColumnQualifierTuple("unit_price", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("orderqty", "sales.salesorderdetail"),
                ColumnQualifierTuple("orderqty", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("weight", "production.product"),
                ColumnQualifierTuple("weight", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("name", "production.product"),
                ColumnQualifierTuple("product_name", "temp.order_line_extract"),
            ),
        ],
        metadata_provider=provider,
    )

    sql = """insert into temp.order_line_extract
select orderqty, unit_price, territoryid as territory, subtotal, color, product_name
from (select salesorderdetailid, orderqty, unitprice as unit_price, specialofferid from sales.salesorderdetail) t1
left join sales.salesorderheader t2 on t1.salesorderid = t2.salesorderid
left join (select productid, color, name as product_name from production.product) t3 on t1.productid = t3.productid
"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("orderqty", "sales.salesorderdetail"),
                ColumnQualifierTuple("orderqty", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("unitprice", "sales.salesorderdetail"),
                ColumnQualifierTuple("unit_price", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("territoryid", "sales.salesorderheader"),
                ColumnQualifierTuple("territory", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("subtotal", "sales.salesorderheader"),
                ColumnQualifierTuple("subtotal", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("color", "production.product"),
                ColumnQualifierTuple("color", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("name", "production.product"),
                ColumnQualifierTuple("product_name", "temp.order_line_extract"),
            ),
        ],
        metadata_provider=provider,
    )


@pytest.mark.parametrize("provider", providers)
def test_select_column_from_tempview_view_subquery(provider: MetaDataProvider):
    sql = """
create or replace view test_view as
select salesorderid, unitprice, productid from sales.salesorderdetail
;

insert into temp.order_line_extract
select test_view.salesorderid, unitprice as unit_price, test_view.productid, shipdate as ship_date, shipmethodid, color, product_name
from test_view
left join sales.salesorderheader t2 on test_view.salesorderid = t2.salesorderid
left join (select productid, color, name as product_name from production.product) t3 on test_view.productid = t3.productid
"""  # noqa: E501
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("salesorderid", "sales.salesorderdetail"),
                ColumnQualifierTuple("salesorderid", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("unitprice", "sales.salesorderdetail"),
                ColumnQualifierTuple("unit_price", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("productid", "sales.salesorderdetail"),
                ColumnQualifierTuple("productid", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("shipdate", "sales.salesorderheader"),
                ColumnQualifierTuple("ship_date", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("shipmethodid", "sales.salesorderheader"),
                ColumnQualifierTuple("shipmethodid", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("color", "production.product"),
                ColumnQualifierTuple("color", "temp.order_line_extract"),
            ),
            (
                ColumnQualifierTuple("name", "production.product"),
                ColumnQualifierTuple("product_name", "temp.order_line_extract"),
            ),
        ],
        metadata_provider=provider,
    )
