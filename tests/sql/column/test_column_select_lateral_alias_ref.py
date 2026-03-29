import os
from unittest.mock import patch

import pytest

from sqllineage.core.metadata_provider import MetaDataProvider
from sqllineage.utils.entities import ColumnQualifierTuple

from ...conftest import data_warehouse_schemas
from ...helpers import assert_column_lineage_equal, generate_metadata_providers

providers = generate_metadata_providers(data_warehouse_schemas)


@pytest.mark.parametrize("provider", providers)
@patch.dict(os.environ, {"SQLLINEAGE_LATERAL_COLUMN_ALIAS_REFERENCE": "1"})
def test_column_top_level_enable_lateral_ref(
    provider: MetaDataProvider,
):
    sql = """
    insert into temp.person_contact
    select
        firstname             as user_name,
        user_name || lastname as id
    from
        person.person
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("firstname", "person.person"),
                ColumnQualifierTuple("user_name", "temp.person_contact"),
            ),
            (
                ColumnQualifierTuple("firstname", "person.person"),
                ColumnQualifierTuple("id", "temp.person_contact"),
            ),
            (
                ColumnQualifierTuple("lastname", "person.person"),
                ColumnQualifierTuple("id", "temp.person_contact"),
            ),
        ],
        test_sqlparse=False,
        metadata_provider=provider,
    )
    sql = """
    insert into temp.person_contact
    (
        firstname,
        middlename
    )
    select
        p.firstname,
        p.firstname || '-' || p.middlename as middlename
    from
        person.person as p
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("firstname", "person.person"),
                ColumnQualifierTuple("firstname", "temp.person_contact"),
            ),
            (
                ColumnQualifierTuple("firstname", "person.person"),
                ColumnQualifierTuple("middlename", "temp.person_contact"),
            ),
            (
                ColumnQualifierTuple("middlename", "person.person"),
                ColumnQualifierTuple("middlename", "temp.person_contact"),
            ),
        ],
        test_sqlparse=False,
        metadata_provider=provider,
    )
    sql = """
    insert into temp.person_identifier
    (
        businessentityid,
        businessentityid_original
    )
    select
        'prefix_' || businessentityid as businessentityid,
        businessentityid              as businessentityid_original
    from
        person.person
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("businessentityid", "person.person"),
                ColumnQualifierTuple("businessentityid", "temp.person_identifier"),
            ),
            (
                ColumnQualifierTuple("businessentityid", "person.person"),
                ColumnQualifierTuple(
                    "businessentityid_original", "temp.person_identifier"
                ),
            ),
        ],
        test_sqlparse=False,
        metadata_provider=provider,
    )


@pytest.mark.parametrize("provider", providers)
@patch.dict(os.environ, {"SQLLINEAGE_LATERAL_COLUMN_ALIAS_REFERENCE": "1"})
def test_column_top_level_enable_lateral_ref_with_metadata_from_table(
    provider: MetaDataProvider,
):
    sql = """
    insert into temp.person_composite_key
    select
        firstname || middlename || lastname || businessentityid as businessentityid,
        businessentityid                                        as businessentityid_original
    from
        person.person
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("firstname", "person.person"),
                ColumnQualifierTuple("businessentityid", "temp.person_composite_key"),
            ),
            (
                ColumnQualifierTuple("middlename", "person.person"),
                ColumnQualifierTuple("businessentityid", "temp.person_composite_key"),
            ),
            (
                ColumnQualifierTuple("lastname", "person.person"),
                ColumnQualifierTuple("businessentityid", "temp.person_composite_key"),
            ),
            (
                ColumnQualifierTuple("businessentityid", "person.person"),
                ColumnQualifierTuple("businessentityid", "temp.person_composite_key"),
            ),
            (
                ColumnQualifierTuple("businessentityid", "person.person"),
                ColumnQualifierTuple(
                    "businessentityid_original", "temp.person_composite_key"
                ),
            ),
        ],
        test_sqlparse=False,
        metadata_provider=provider,
    )


@pytest.mark.parametrize("provider", providers)
@patch.dict(os.environ, {"SQLLINEAGE_LATERAL_COLUMN_ALIAS_REFERENCE": "1"})
def test_column_top_level_enable_lateral_ref_with_metadata_from_subquery(
    provider: MetaDataProvider,
):
    sql = """
    insert into temp.person_composite_key
    select
        firstname || middlename || lastname || businessentityid as businessentityid,
        businessentityid                                        as businessentityid_original
    from
        (
            select
               *
            from
                person.person
        ) as sq
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("firstname", "person.person"),
                ColumnQualifierTuple("businessentityid", "temp.person_composite_key"),
            ),
            (
                ColumnQualifierTuple("middlename", "person.person"),
                ColumnQualifierTuple("businessentityid", "temp.person_composite_key"),
            ),
            (
                ColumnQualifierTuple("lastname", "person.person"),
                ColumnQualifierTuple("businessentityid", "temp.person_composite_key"),
            ),
            (
                ColumnQualifierTuple("businessentityid", "person.person"),
                ColumnQualifierTuple("businessentityid", "temp.person_composite_key"),
            ),
            (
                ColumnQualifierTuple("businessentityid", "person.person"),
                ColumnQualifierTuple(
                    "businessentityid_original", "temp.person_composite_key"
                ),
            ),
        ],
        test_sqlparse=False,
        metadata_provider=provider,
    )


@pytest.mark.parametrize("provider", providers)
@patch.dict(os.environ, {"SQLLINEAGE_LATERAL_COLUMN_ALIAS_REFERENCE": "1"})
def test_column_top_level_enable_lateral_ref_with_metadata_from_nested_subquery(
    provider: MetaDataProvider,
):
    sql = """
    insert into temp.person_composite_key
    select
        firstname || middlename || lastname || businessentityid as businessentityid,
        businessentityid                                        as businessentityid_original
    from
        (
            select
                *
            from
                (
                    select
                       *
                    from
                        person.person
                ) as inner_sq
        ) as outer_sq
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("firstname", "person.person"),
                ColumnQualifierTuple("businessentityid", "temp.person_composite_key"),
            ),
            (
                ColumnQualifierTuple("middlename", "person.person"),
                ColumnQualifierTuple("businessentityid", "temp.person_composite_key"),
            ),
            (
                ColumnQualifierTuple("lastname", "person.person"),
                ColumnQualifierTuple("businessentityid", "temp.person_composite_key"),
            ),
            (
                ColumnQualifierTuple("businessentityid", "person.person"),
                ColumnQualifierTuple("businessentityid", "temp.person_composite_key"),
            ),
            (
                ColumnQualifierTuple("businessentityid", "person.person"),
                ColumnQualifierTuple(
                    "businessentityid_original", "temp.person_composite_key"
                ),
            ),
        ],
        test_sqlparse=False,
        metadata_provider=provider,
    )


@pytest.mark.parametrize("provider", providers)
@patch.dict(os.environ, {"SQLLINEAGE_LATERAL_COLUMN_ALIAS_REFERENCE": "1"})
def test_column_enable_lateral_ref_within_subquery(
    provider: MetaDataProvider,
):
    sql = """
    insert into temp.person_full_name
    select
        sq.name
    from
        (
            select
                firstname  || middlename as prefixname,
                prefixname || lastname   as name
            from
                person.person
        ) as sq
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("firstname", "person.person"),
                ColumnQualifierTuple("name", "temp.person_full_name"),
            ),
            (
                ColumnQualifierTuple("middlename", "person.person"),
                ColumnQualifierTuple("name", "temp.person_full_name"),
            ),
            (
                ColumnQualifierTuple("lastname", "person.person"),
                ColumnQualifierTuple("name", "temp.person_full_name"),
            ),
        ],
        test_sqlparse=False,
        metadata_provider=provider,
    )

    sql = """
    insert into temp.customer_identifier
    select
        sq.customer_code
    from
        (
            select
                p.businessentityid || p.lastname   as partial_code,
                partial_code       || c.customerid as customer_code
            from
                person.person as p
                join
                    sales.customer as c
                on
                    p.businessentityid = c.personid
        ) as sq
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("businessentityid", "person.person"),
                ColumnQualifierTuple("customer_code", "temp.customer_identifier"),
            ),
            (
                ColumnQualifierTuple("lastname", "person.person"),
                ColumnQualifierTuple("customer_code", "temp.customer_identifier"),
            ),
            (
                ColumnQualifierTuple("customerid", "sales.customer"),
                ColumnQualifierTuple("customer_code", "temp.customer_identifier"),
            ),
        ],
        test_sqlparse=False,
        metadata_provider=provider,
    )


def test_column_top_level_disable_lateral_ref():
    sql = """
    insert into temp.person_contact
    select
        firstname             as user_name,
        user_name || lastname as id
    from
        person.person
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("firstname", "person.person"),
                ColumnQualifierTuple("user_name", "temp.person_contact"),
            ),
            (
                ColumnQualifierTuple("user_name", "person.person"),
                ColumnQualifierTuple("id", "temp.person_contact"),
            ),
            (
                ColumnQualifierTuple("lastname", "person.person"),
                ColumnQualifierTuple("id", "temp.person_contact"),
            ),
        ],
        test_sqlparse=False,
    )
    sql = """
    insert into temp.person_contact
    (
        firstname,
        middlename
    )
    select
        p.firstname,
        p.firstname || '-' || p.middlename as middlename
    from
        person.person as p
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("firstname", "person.person"),
                ColumnQualifierTuple("firstname", "temp.person_contact"),
            ),
            (
                ColumnQualifierTuple("firstname", "person.person"),
                ColumnQualifierTuple("middlename", "temp.person_contact"),
            ),
            (
                ColumnQualifierTuple("middlename", "person.person"),
                ColumnQualifierTuple("middlename", "temp.person_contact"),
            ),
        ],
        test_sqlparse=False,
    )
    sql = """
    insert into temp.person_identifier
    (
        businessentityid,
        businessentityid_original
    )
    select
        'prefix_' || businessentityid as businessentityid,
        businessentityid              as businessentityid_original
    from
        person.person
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("businessentityid", "person.person"),
                ColumnQualifierTuple("businessentityid", "temp.person_identifier"),
            ),
            (
                ColumnQualifierTuple("businessentityid", "person.person"),
                ColumnQualifierTuple(
                    "businessentityid_original", "temp.person_identifier"
                ),
            ),
        ],
        test_sqlparse=False,
    )


def test_column_top_level_disable_lateral_ref_with_metadata_from_table():
    sql = """
        insert into temp.person_composite_key
        select
            firstname || middlename || lastname || businessentityid as businessentityid,
            businessentityid                                        as businessentityid_original
        from
            person.person
        """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("firstname", "person.person"),
                ColumnQualifierTuple("businessentityid", "temp.person_composite_key"),
            ),
            (
                ColumnQualifierTuple("middlename", "person.person"),
                ColumnQualifierTuple("businessentityid", "temp.person_composite_key"),
            ),
            (
                ColumnQualifierTuple("lastname", "person.person"),
                ColumnQualifierTuple("businessentityid", "temp.person_composite_key"),
            ),
            (
                ColumnQualifierTuple("businessentityid", "person.person"),
                ColumnQualifierTuple("businessentityid", "temp.person_composite_key"),
            ),
            (
                ColumnQualifierTuple("businessentityid", "person.person"),
                ColumnQualifierTuple(
                    "businessentityid_original", "temp.person_composite_key"
                ),
            ),
        ],
        test_sqlparse=False,
    )


def test_column_disable_lateral_ref_within_subquery():
    sql = """
    insert into temp.person_full_name
    select
        sq.name
    from
        (
            select
                firstname  || middlename as prefixname,
                prefixname || lastname   as name
            from
                person.person
        ) as sq
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("prefixname", "person.person"),
                ColumnQualifierTuple("name", "temp.person_full_name"),
            ),
            (
                ColumnQualifierTuple("lastname", "person.person"),
                ColumnQualifierTuple("name", "temp.person_full_name"),
            ),
        ],
        test_sqlparse=False,
    )

    sql = """
    insert into temp.customer_identifier
    select
        sq.customer_code
    from
        (
            select
                p.businessentityid || p.lastname   as partial_code,
                partial_code       || c.customerid as customer_code
            from
                person.person as p
                join
                    sales.customer as c
                on
                    p.businessentityid = c.personid
        ) as sq
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("partial_code", None),
                ColumnQualifierTuple("customer_code", "temp.customer_identifier"),
            ),
            (
                ColumnQualifierTuple("customerid", "sales.customer"),
                ColumnQualifierTuple("customer_code", "temp.customer_identifier"),
            ),
        ],
        test_sqlparse=False,
    )
