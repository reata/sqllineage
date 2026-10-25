import pytest

from sqllineage.core.metadata_provider import MetaDataProvider
from sqllineage.utils.entities import ColumnQualifierTuple

from ...helpers import assert_column_lineage_equal


@pytest.mark.parametrize("dialect", ["oracle", "snowflake"])
def test_insert_all_with_values(dialect: str):
    sql = """INSERT ALL
    INTO tab1 (col1, col2) VALUES (col3, col4 + col5)
    INTO tab2 VALUES (col3, 1)
SELECT col1 AS col3, col4, col5 FROM tab3"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab3"),
                ColumnQualifierTuple("col1", "tab1"),
            ),
            (
                ColumnQualifierTuple("col4", "tab3"),
                ColumnQualifierTuple("col2", "tab1"),
            ),
            (
                ColumnQualifierTuple("col5", "tab3"),
                ColumnQualifierTuple("col2", "tab1"),
            ),
            (
                ColumnQualifierTuple("col1", "tab3"),
                ColumnQualifierTuple("col3", "tab2"),
            ),
        ],
        dialect=dialect,
        test_sqlparse=False,
    )


def test_insert_all_without_values():
    sql = """INSERT ALL
    INTO tab1
    INTO tab2 (col3, col4)
SELECT t3.col1, t4.col2 FROM tab3 t3 JOIN tab4 t4 ON t3.id = t4.id"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col1", "tab3"),
                ColumnQualifierTuple("col1", "tab1"),
            ),
            (
                ColumnQualifierTuple("col2", "tab4"),
                ColumnQualifierTuple("col2", "tab1"),
            ),
            (
                ColumnQualifierTuple("col1", "tab3"),
                ColumnQualifierTuple("col3", "tab2"),
            ),
            (
                ColumnQualifierTuple("col2", "tab4"),
                ColumnQualifierTuple("col4", "tab2"),
            ),
        ],
        dialect="snowflake",
        test_sqlparse=False,
    )


def test_insert_first_with_conditions():
    sql = """INSERT FIRST
    WHEN amount > 100 THEN
        INTO large_orders (order_id, amount) VALUES (id, amount)
    WHEN amount > 10 THEN
        INTO medium_orders (order_id) VALUES (id)
    ELSE
        INTO small_orders
SELECT o.id, o.amount FROM orders o"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("id", "orders"),
                ColumnQualifierTuple("order_id", "large_orders"),
            ),
            (
                ColumnQualifierTuple("amount", "orders"),
                ColumnQualifierTuple("amount", "large_orders"),
            ),
            (
                ColumnQualifierTuple("id", "orders"),
                ColumnQualifierTuple("order_id", "medium_orders"),
            ),
            (
                ColumnQualifierTuple("id", "orders"),
                ColumnQualifierTuple("id", "small_orders"),
            ),
            (
                ColumnQualifierTuple("amount", "orders"),
                ColumnQualifierTuple("amount", "small_orders"),
            ),
        ],
        dialect="snowflake",
        test_sqlparse=False,
    )


def test_insert_all_from_cte():
    sql = """INSERT ALL
    INTO tab1 (col1) VALUES (col2)
    INTO tab3 (col4) VALUES (col2)
WITH cte1 AS (SELECT col3 AS col2 FROM tab2)
SELECT col2 FROM cte1"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("col3", "tab2"),
                ColumnQualifierTuple("col1", "tab1"),
            ),
            (
                ColumnQualifierTuple("col3", "tab2"),
                ColumnQualifierTuple("col4", "tab3"),
            ),
        ],
        dialect="oracle",
        test_sqlparse=False,
    )


def test_insert_all_target_column_from_metadata(provider: MetaDataProvider):
    sql = """INSERT ALL
    INTO marts.dim_credit_card VALUES (card_key, creditcardid, cardtype)
SELECT cardtype, creditcardid, md5(creditcardid) AS card_key
FROM sales.creditcard"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("creditcardid", "sales.creditcard"),
                ColumnQualifierTuple("creditcard_key", "marts.dim_credit_card"),
            ),
            (
                ColumnQualifierTuple("creditcardid", "sales.creditcard"),
                ColumnQualifierTuple("creditcardid", "marts.dim_credit_card"),
            ),
            (
                ColumnQualifierTuple("cardtype", "sales.creditcard"),
                ColumnQualifierTuple("cardtype", "marts.dim_credit_card"),
            ),
        ],
        dialect="snowflake",
        metadata_provider=provider,
        test_sqlparse=False,
    )


def test_insert_all_wildcard_from_metadata(provider: MetaDataProvider):
    sql = """INSERT ALL
    INTO temp.creditcard_snapshot
    INTO temp.creditcard_archive
SELECT * FROM marts.dim_credit_card"""
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple(col, "marts.dim_credit_card"),
                ColumnQualifierTuple(col, tgt),
            )
            for col in ["creditcard_key", "creditcardid", "cardtype"]
            for tgt in ["temp.creditcard_snapshot", "temp.creditcard_archive"]
        ],
        dialect="snowflake",
        metadata_provider=provider,
        test_sqlparse=False,
    )
