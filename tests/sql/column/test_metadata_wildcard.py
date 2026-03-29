import pytest

from sqllineage.core.metadata_provider import MetaDataProvider
from sqllineage.utils.entities import ColumnQualifierTuple

from ...conftest import data_warehouse_schemas
from ...helpers import assert_column_lineage_equal, generate_metadata_providers

providers = generate_metadata_providers(data_warehouse_schemas)


@pytest.mark.parametrize("provider", providers)
def test_select_single_table_wildcard(provider: MetaDataProvider):
    sql = """insert into temp.creditcard_snapshot
    select *
    from marts.dim_credit_card
    """
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
        metadata_provider=provider,
    )


@pytest.mark.parametrize("provider", providers)
def test_select_single_table_wildcard_in_subquery(provider: MetaDataProvider):
    sql = """insert into temp.creditcard_snapshot
    select creditcard_key, cardtype
    from (
        select *
        from (
            select creditcardid, cardtype, creditcard_key from marts.dim_credit_card
        ) t1
    ) t2
    """
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
        metadata_provider=provider,
    )


@pytest.mark.parametrize("provider", providers)
def test_select_single_table_partial_wildcard_in_subquery(provider: MetaDataProvider):
    sql = """insert into temp.creditcard_snapshot
    select creditcardid, cardtype
    from (
        select *, row_number() over (partition by creditcard_key) as rn
        from marts.dim_credit_card
    ) t1
    where rn = 1
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("creditcardid", "marts.dim_credit_card"),
                ColumnQualifierTuple("creditcardid", "temp.creditcard_snapshot"),
            ),
            (
                ColumnQualifierTuple("cardtype", "marts.dim_credit_card"),
                ColumnQualifierTuple("cardtype", "temp.creditcard_snapshot"),
            ),
        ],
        metadata_provider=provider,
    )
    sql = """insert into temp.creditcard_snapshot
    select creditcardid, cardtype
    from (
        select *, row_number() over (partition by creditcardid) as rn
        from marts.dim_credit_card t
    ) t1
    where rn = 1
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("creditcardid", "marts.dim_credit_card"),
                ColumnQualifierTuple("creditcardid", "temp.creditcard_snapshot"),
            ),
            (
                ColumnQualifierTuple("cardtype", "marts.dim_credit_card"),
                ColumnQualifierTuple("cardtype", "temp.creditcard_snapshot"),
            ),
        ],
        metadata_provider=provider,
    )


@pytest.mark.parametrize("provider", providers)
def test_select_table_join_partial_wildcard_at_last(provider: MetaDataProvider):
    sql = """insert into temp.sales_creditcard_snapshot
    select sales_key, creditcard_key, cardtype
    from (
        select x.sales_key, y.* from marts.fct_sales x
        join marts.dim_credit_card y on x.creditcard_key = y.creditcard_key
    ) t
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("sales_key", "marts.fct_sales"),
                ColumnQualifierTuple("sales_key", "temp.sales_creditcard_snapshot"),
            ),
            (
                ColumnQualifierTuple("creditcard_key", "marts.dim_credit_card"),
                ColumnQualifierTuple(
                    "creditcard_key", "temp.sales_creditcard_snapshot"
                ),
            ),
            (
                ColumnQualifierTuple("cardtype", "marts.dim_credit_card"),
                ColumnQualifierTuple("cardtype", "temp.sales_creditcard_snapshot"),
            ),
        ],
        metadata_provider=provider,
    )


@pytest.mark.parametrize("provider", providers)
def test_select_table_join_partial_wildcard_at_beginning(provider: MetaDataProvider):
    sql = """insert into temp.sales_creditcard_snapshot
    select sales_key, creditcard_key, cardtype
    from (
        select x.*, y.cardtype from marts.fct_sales x
        join marts.dim_credit_card y on x.creditcard_key = y.creditcard_key
    ) t
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("sales_key", "marts.fct_sales"),
                ColumnQualifierTuple("sales_key", "temp.sales_creditcard_snapshot"),
            ),
            (
                ColumnQualifierTuple("creditcard_key", "marts.fct_sales"),
                ColumnQualifierTuple(
                    "creditcard_key", "temp.sales_creditcard_snapshot"
                ),
            ),
            (
                ColumnQualifierTuple("cardtype", "marts.dim_credit_card"),
                ColumnQualifierTuple("cardtype", "temp.sales_creditcard_snapshot"),
            ),
        ],
        metadata_provider=provider,
    )


@pytest.mark.parametrize("provider", providers)
def test_select_table_join_multiple_wildcards(provider: MetaDataProvider):
    sql = """insert into temp.sales_creditcard_snapshot
    select sales_key, cardtype
    from (
        select x.*, y.* from marts.fct_sales x
        join marts.dim_credit_card y on x.creditcard_key = y.creditcard_key
    ) t
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("sales_key", "marts.fct_sales"),
                ColumnQualifierTuple("sales_key", "temp.sales_creditcard_snapshot"),
            ),
            (
                ColumnQualifierTuple("cardtype", "marts.dim_credit_card"),
                ColumnQualifierTuple("cardtype", "temp.sales_creditcard_snapshot"),
            ),
        ],
        metadata_provider=provider,
    )


@pytest.mark.parametrize("provider", providers)
def test_select_table_join_multiple_wildcards_merged_at_top_level(
    provider: MetaDataProvider,
):
    sql = """insert into temp.sales_ship_address_snapshot
    select *
    from (
        select x.*, z.* from marts.fct_sales x
        join marts.dim_address z on x.ship_address_key = z.address_key
    ) t
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("sales_key", "marts.fct_sales"),
                ColumnQualifierTuple("sales_key", "temp.sales_ship_address_snapshot"),
            ),
            (
                ColumnQualifierTuple("product_key", "marts.fct_sales"),
                ColumnQualifierTuple("product_key", "temp.sales_ship_address_snapshot"),
            ),
            (
                ColumnQualifierTuple("customer_key", "marts.fct_sales"),
                ColumnQualifierTuple(
                    "customer_key", "temp.sales_ship_address_snapshot"
                ),
            ),
            (
                ColumnQualifierTuple("creditcard_key", "marts.fct_sales"),
                ColumnQualifierTuple(
                    "creditcard_key", "temp.sales_ship_address_snapshot"
                ),
            ),
            (
                ColumnQualifierTuple("ship_address_key", "marts.fct_sales"),
                ColumnQualifierTuple(
                    "ship_address_key", "temp.sales_ship_address_snapshot"
                ),
            ),
            (
                ColumnQualifierTuple("order_status_key", "marts.fct_sales"),
                ColumnQualifierTuple(
                    "order_status_key", "temp.sales_ship_address_snapshot"
                ),
            ),
            (
                ColumnQualifierTuple("order_date_key", "marts.fct_sales"),
                ColumnQualifierTuple(
                    "order_date_key", "temp.sales_ship_address_snapshot"
                ),
            ),
            (
                ColumnQualifierTuple("salesorderid", "marts.fct_sales"),
                ColumnQualifierTuple(
                    "salesorderid", "temp.sales_ship_address_snapshot"
                ),
            ),
            (
                ColumnQualifierTuple("salesorderdetailid", "marts.fct_sales"),
                ColumnQualifierTuple(
                    "salesorderdetailid", "temp.sales_ship_address_snapshot"
                ),
            ),
            (
                ColumnQualifierTuple("unitprice", "marts.fct_sales"),
                ColumnQualifierTuple("unitprice", "temp.sales_ship_address_snapshot"),
            ),
            (
                ColumnQualifierTuple("orderqty", "marts.fct_sales"),
                ColumnQualifierTuple("orderqty", "temp.sales_ship_address_snapshot"),
            ),
            (
                ColumnQualifierTuple("revenue", "marts.fct_sales"),
                ColumnQualifierTuple("revenue", "temp.sales_ship_address_snapshot"),
            ),
            (
                ColumnQualifierTuple("address_key", "marts.dim_address"),
                ColumnQualifierTuple("address_key", "temp.sales_ship_address_snapshot"),
            ),
            (
                ColumnQualifierTuple("addressid", "marts.dim_address"),
                ColumnQualifierTuple("addressid", "temp.sales_ship_address_snapshot"),
            ),
            (
                ColumnQualifierTuple("city_name", "marts.dim_address"),
                ColumnQualifierTuple("city_name", "temp.sales_ship_address_snapshot"),
            ),
            (
                ColumnQualifierTuple("state_name", "marts.dim_address"),
                ColumnQualifierTuple("state_name", "temp.sales_ship_address_snapshot"),
            ),
            (
                ColumnQualifierTuple("country_name", "marts.dim_address"),
                ColumnQualifierTuple(
                    "country_name", "temp.sales_ship_address_snapshot"
                ),
            ),
        ],
        metadata_provider=provider,
    )


@pytest.mark.parametrize("provider", providers)
def test_select_table_join_multiple_wildcards_at_different_level(
    provider: MetaDataProvider,
):
    sql = """insert into temp.sales_ship_address_snapshot
    select x.*, y.addressid, y.address_key
    from (select * from marts.fct_sales where revenue >= 100) x
    join (select *
          from (
              select *, row_number() over (partition by address_key) as rn from marts.dim_address
          ) t1
          where rn = 1
    ) y
    on x.ship_address_key = y.address_key
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("sales_key", "marts.fct_sales"),
                ColumnQualifierTuple("sales_key", "temp.sales_ship_address_snapshot"),
            ),
            (
                ColumnQualifierTuple("product_key", "marts.fct_sales"),
                ColumnQualifierTuple("product_key", "temp.sales_ship_address_snapshot"),
            ),
            (
                ColumnQualifierTuple("customer_key", "marts.fct_sales"),
                ColumnQualifierTuple(
                    "customer_key", "temp.sales_ship_address_snapshot"
                ),
            ),
            (
                ColumnQualifierTuple("creditcard_key", "marts.fct_sales"),
                ColumnQualifierTuple(
                    "creditcard_key", "temp.sales_ship_address_snapshot"
                ),
            ),
            (
                ColumnQualifierTuple("ship_address_key", "marts.fct_sales"),
                ColumnQualifierTuple(
                    "ship_address_key", "temp.sales_ship_address_snapshot"
                ),
            ),
            (
                ColumnQualifierTuple("order_status_key", "marts.fct_sales"),
                ColumnQualifierTuple(
                    "order_status_key", "temp.sales_ship_address_snapshot"
                ),
            ),
            (
                ColumnQualifierTuple("order_date_key", "marts.fct_sales"),
                ColumnQualifierTuple(
                    "order_date_key", "temp.sales_ship_address_snapshot"
                ),
            ),
            (
                ColumnQualifierTuple("salesorderid", "marts.fct_sales"),
                ColumnQualifierTuple(
                    "salesorderid", "temp.sales_ship_address_snapshot"
                ),
            ),
            (
                ColumnQualifierTuple("salesorderdetailid", "marts.fct_sales"),
                ColumnQualifierTuple(
                    "salesorderdetailid", "temp.sales_ship_address_snapshot"
                ),
            ),
            (
                ColumnQualifierTuple("unitprice", "marts.fct_sales"),
                ColumnQualifierTuple("unitprice", "temp.sales_ship_address_snapshot"),
            ),
            (
                ColumnQualifierTuple("orderqty", "marts.fct_sales"),
                ColumnQualifierTuple("orderqty", "temp.sales_ship_address_snapshot"),
            ),
            (
                ColumnQualifierTuple("revenue", "marts.fct_sales"),
                ColumnQualifierTuple("revenue", "temp.sales_ship_address_snapshot"),
            ),
            (
                ColumnQualifierTuple("addressid", "marts.dim_address"),
                ColumnQualifierTuple("addressid", "temp.sales_ship_address_snapshot"),
            ),
            (
                ColumnQualifierTuple("address_key", "marts.dim_address"),
                ColumnQualifierTuple("address_key", "temp.sales_ship_address_snapshot"),
            ),
        ],
        metadata_provider=provider,
    )


@pytest.mark.parametrize("provider", providers)
def test_wildcard_reference_from_previous_statements(provider: MetaDataProvider):
    sql = """create table temp.customer_latest_ship_address_snapshot as
    select customer_key, ship_address_key
    from (
        select *, row_number() over (partition by customer_key) as rn
        from marts.fct_sales t
    ) t1
    where rn = 1
    ;

    create table temp.address_snapshot as
    select *
    from marts.dim_address
    ;

    insert into temp.customer_address_snapshot
    select t1.*, t2.fullname, t3.*
    from temp.customer_latest_ship_address_snapshot t1
    join (select * from marts.dim_customer where fullname is not null) t2 on t1.customer_key = t2.customer_key
    left join temp.address_snapshot t3 on t1.ship_address_key = t3.address_key
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("customer_key", "marts.fct_sales"),
                ColumnQualifierTuple("customer_key", "temp.customer_address_snapshot"),
            ),
            (
                ColumnQualifierTuple("ship_address_key", "marts.fct_sales"),
                ColumnQualifierTuple(
                    "ship_address_key", "temp.customer_address_snapshot"
                ),
            ),
            (
                ColumnQualifierTuple("fullname", "marts.dim_customer"),
                ColumnQualifierTuple("fullname", "temp.customer_address_snapshot"),
            ),
            (
                ColumnQualifierTuple("address_key", "marts.dim_address"),
                ColumnQualifierTuple("address_key", "temp.customer_address_snapshot"),
            ),
            (
                ColumnQualifierTuple("addressid", "marts.dim_address"),
                ColumnQualifierTuple("addressid", "temp.customer_address_snapshot"),
            ),
            (
                ColumnQualifierTuple("city_name", "marts.dim_address"),
                ColumnQualifierTuple("city_name", "temp.customer_address_snapshot"),
            ),
            (
                ColumnQualifierTuple("state_name", "marts.dim_address"),
                ColumnQualifierTuple("state_name", "temp.customer_address_snapshot"),
            ),
            (
                ColumnQualifierTuple("country_name", "marts.dim_address"),
                ColumnQualifierTuple("country_name", "temp.customer_address_snapshot"),
            ),
        ],
        metadata_provider=provider,
    )


@pytest.mark.parametrize(
    "provider",
    generate_metadata_providers(
        {
            **data_warehouse_schemas,
            "temp.dim_credit_card1": ["creditcard_key", "creditcardid", "cardtype"],
        }
    ),
)
def test_wildcard_table_union_with_same_column_names(provider: MetaDataProvider):
    sql = """insert into temp.creditcard_snapshot
SELECT * FROM marts.dim_credit_card
UNION
SELECT * FROM temp.dim_credit_card1"""
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
            (
                ColumnQualifierTuple("creditcard_key", "temp.dim_credit_card1"),
                ColumnQualifierTuple("creditcard_key", "temp.creditcard_snapshot"),
            ),
            (
                ColumnQualifierTuple("creditcardid", "temp.dim_credit_card1"),
                ColumnQualifierTuple("creditcardid", "temp.creditcard_snapshot"),
            ),
            (
                ColumnQualifierTuple("cardtype", "temp.dim_credit_card1"),
                ColumnQualifierTuple("cardtype", "temp.creditcard_snapshot"),
            ),
        ],
        metadata_provider=provider,
    )


@pytest.mark.parametrize(
    "provider",
    generate_metadata_providers(
        {
            **data_warehouse_schemas,
            "temp.dim_credit_card2": ["cc_key", "cc_id", "cc_type"],
        }
    ),
)
def test_wildcard_table_union_with_different_column_names(provider: MetaDataProvider):
    sql = """insert into temp.creditcard_snapshot
SELECT * FROM marts.dim_credit_card
UNION
SELECT * FROM temp.dim_credit_card2"""
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
            (
                ColumnQualifierTuple("cc_key", "temp.dim_credit_card2"),
                ColumnQualifierTuple("creditcard_key", "temp.creditcard_snapshot"),
            ),
            (
                ColumnQualifierTuple("cc_id", "temp.dim_credit_card2"),
                ColumnQualifierTuple("creditcardid", "temp.creditcard_snapshot"),
            ),
            (
                ColumnQualifierTuple("cc_type", "temp.dim_credit_card2"),
                ColumnQualifierTuple("cardtype", "temp.creditcard_snapshot"),
            ),
        ],
        metadata_provider=provider,
    )


@pytest.mark.parametrize(
    "provider",
    generate_metadata_providers(
        {
            **data_warehouse_schemas,
            "temp.credit_card_daily": ["creditcard_key", "creditcard_id", "card_type"],
        }
    ),
)
@pytest.mark.parametrize("dialect", ["databricks", "hive", "sparksql"])
def test_insert_overwrite_with_partition_excludes_partition_columns(
    provider: MetaDataProvider,
    dialect: str,
):
    sql = """INSERT OVERWRITE TABLE temp.credit_card_daily PARTITION (dt=20251217)
SELECT creditcard_key, creditcardid, cardtype
FROM marts.dim_credit_card"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("creditcard_key", "marts.dim_credit_card"),
                ColumnQualifierTuple("creditcard_key", "temp.credit_card_daily"),
            ),
            (
                ColumnQualifierTuple("creditcardid", "marts.dim_credit_card"),
                ColumnQualifierTuple("creditcard_id", "temp.credit_card_daily"),
            ),
            (
                ColumnQualifierTuple("cardtype", "marts.dim_credit_card"),
                ColumnQualifierTuple("card_type", "temp.credit_card_daily"),
            ),
        ],
        dialect=dialect,
        metadata_provider=provider,
        test_sqlparse=False,  # Only test with sqlfluff as sqlparse doesn't handle write_column
    )
