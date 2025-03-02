import os
from unittest.mock import patch

import pytest

from sqllineage.core.metadata_provider import MetaDataProvider
from sqllineage.utils.entities import ColumnQualifierTuple

from ...helpers import assert_column_lineage_equal, generate_metadata_providers

providers = generate_metadata_providers(
    {
        "public.src_tbl1": ["id", "a", "b", "c"],
    }
)


@pytest.mark.parametrize("provider", providers)
@patch.dict(os.environ, {"SQLLINEAGE_LATERAL_COLUMN_ALIAS_REFERENCE": "1"})
def test_column_top_level_enable_lateral_ref(
    provider: MetaDataProvider,
):
    sql = """
    insert into public.tgt_tbl1
    select
        name               as user_name,
        user_name || email as id
    from
        public.src_tbl1
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("name", "public.src_tbl1"),
                ColumnQualifierTuple("user_name", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("name", "public.src_tbl1"),
                ColumnQualifierTuple("id", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("email", "public.src_tbl1"),
                ColumnQualifierTuple("id", "public.tgt_tbl1"),
            ),
        ],
        test_sqlparse=False,
        metadata_provider=provider,
    )
    sql = """
    insert into public.tgt_tbl1
    (
        name,
        email
    )
    select
        st1.name,
        st1.name || st1.email || '@gmail.com' as email
    from
        public.src_tbl1 as st1
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("name", "public.src_tbl1"),
                ColumnQualifierTuple("name", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("name", "public.src_tbl1"),
                ColumnQualifierTuple("email", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("email", "public.src_tbl1"),
                ColumnQualifierTuple("email", "public.tgt_tbl1"),
            ),
        ],
        test_sqlparse=False,
        metadata_provider=provider,
    )
    sql = """
    insert into public.tgt_tbl1
    (
        id,
        id_original
    )
    select
        'a || b || c' || id as id,
        id                  as id_original
    from
        public.src_tbl1
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("id", "public.src_tbl1"),
                ColumnQualifierTuple("id", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("id", "public.src_tbl1"),
                ColumnQualifierTuple("id_original", "public.tgt_tbl1"),
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
    insert into public.tgt_tbl1
    select
        a || b || c || id as id,
        id                as id_original
    from
        public.src_tbl1
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("a", "public.src_tbl1"),
                ColumnQualifierTuple("id", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("b", "public.src_tbl1"),
                ColumnQualifierTuple("id", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("c", "public.src_tbl1"),
                ColumnQualifierTuple("id", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("id", "public.src_tbl1"),
                ColumnQualifierTuple("id", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("id", "public.src_tbl1"),
                ColumnQualifierTuple("id_original", "public.tgt_tbl1"),
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
    insert into public.tgt_tbl1
    select
        a || b || c || id as id,
        id                as id_original
    from
        (
            select
               *
            from
                public.src_tbl1
        ) as sq
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("a", "public.src_tbl1"),
                ColumnQualifierTuple("id", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("b", "public.src_tbl1"),
                ColumnQualifierTuple("id", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("c", "public.src_tbl1"),
                ColumnQualifierTuple("id", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("id", "public.src_tbl1"),
                ColumnQualifierTuple("id", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("id", "public.src_tbl1"),
                ColumnQualifierTuple("id_original", "public.tgt_tbl1"),
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
    insert into public.tgt_tbl1
    select
        a || b || c || id as id,
        id                as id_original
    from
        (
            select
                *
            from
                (
                    select
                       *
                    from
                        public.src_tbl1
                ) as inner_sq
        ) as outer_sq
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("a", "public.src_tbl1"),
                ColumnQualifierTuple("id", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("b", "public.src_tbl1"),
                ColumnQualifierTuple("id", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("c", "public.src_tbl1"),
                ColumnQualifierTuple("id", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("id", "public.src_tbl1"),
                ColumnQualifierTuple("id", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("id", "public.src_tbl1"),
                ColumnQualifierTuple("id_original", "public.tgt_tbl1"),
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
    insert into public.tgt_tbl1
    select
        sq.name
    from
        (
            select
                id     || a as alias1,
                alias1 || b as name
            from
                public.src_tbl1
        ) as sq
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("id", "public.src_tbl1"),
                ColumnQualifierTuple("name", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("a", "public.src_tbl1"),
                ColumnQualifierTuple("name", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("b", "public.src_tbl1"),
                ColumnQualifierTuple("name", "public.tgt_tbl1"),
            ),
        ],
        test_sqlparse=False,
        metadata_provider=provider,
    )

    sql = """
    insert into public.tgt_tbl1
    select
        sq.name
    from
        (
            select
                st1.id || st1.name as alias1,
                alias1 || st2.email as name
            from
                public.src_tbl1 as st1
                join
                    public.src_tbl2 as st2
                on
                    st1.id = st2.id
        ) as sq
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("id", "public.src_tbl1"),
                ColumnQualifierTuple("name", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("name", "public.src_tbl1"),
                ColumnQualifierTuple("name", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("email", "public.src_tbl2"),
                ColumnQualifierTuple("name", "public.tgt_tbl1"),
            ),
        ],
        test_sqlparse=False,
        metadata_provider=provider,
    )


def test_column_top_level_disable_lateral_ref():
    sql = """
    insert into public.tgt_tbl1
    select
        name               as user_name,
        user_name || email as id
    from
        public.src_tbl1
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("name", "public.src_tbl1"),
                ColumnQualifierTuple("user_name", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("user_name", "public.src_tbl1"),
                ColumnQualifierTuple("id", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("email", "public.src_tbl1"),
                ColumnQualifierTuple("id", "public.tgt_tbl1"),
            ),
        ],
        test_sqlparse=False,
    )
    sql = """
    insert into public.tgt_tbl1
    (
        name,
        email
    )
    select
        st1.name,
        st1.name || st1.email || '@gmail.com' as email
    from
        public.src_tbl1 as st1
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("name", "public.src_tbl1"),
                ColumnQualifierTuple("name", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("name", "public.src_tbl1"),
                ColumnQualifierTuple("email", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("email", "public.src_tbl1"),
                ColumnQualifierTuple("email", "public.tgt_tbl1"),
            ),
        ],
        test_sqlparse=False,
    )
    sql = """
    insert into public.tgt_tbl1
    (
        id,
        id_original
    )
    select
        'a || b || c' || id as id,
        id                  as id_original
    from
        public.src_tbl1
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("id", "public.src_tbl1"),
                ColumnQualifierTuple("id", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("id", "public.src_tbl1"),
                ColumnQualifierTuple("id_original", "public.tgt_tbl1"),
            ),
        ],
        test_sqlparse=False,
    )


def test_column_top_level_disable_lateral_ref_with_metadata_from_table():
    sql = """
    insert into public.tgt_tbl1
    select
        a || b || c || id as id,
        id                as id_original
    from
        public.src_tbl1
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("a", "public.src_tbl1"),
                ColumnQualifierTuple("id", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("b", "public.src_tbl1"),
                ColumnQualifierTuple("id", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("c", "public.src_tbl1"),
                ColumnQualifierTuple("id", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("id", "public.src_tbl1"),
                ColumnQualifierTuple("id", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("id", "public.src_tbl1"),
                ColumnQualifierTuple("id_original", "public.tgt_tbl1"),
            ),
        ],
        test_sqlparse=False,
    )


def test_column_disable_lateral_ref_within_subquery():
    sql = """
    insert into public.tgt_tbl1
    select
        sq.name
    from
        (
            select
                id     || a as alias1,
                alias1 || b as name
            from
                public.src_tbl1
        ) as sq
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("alias1", "public.src_tbl1"),
                ColumnQualifierTuple("name", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("b", "public.src_tbl1"),
                ColumnQualifierTuple("name", "public.tgt_tbl1"),
            ),
        ],
        test_sqlparse=False,
    )

    sql = """
    insert into public.tgt_tbl1
    select
        sq.name
    from
        (
            select
                st1.id || st1.name as alias1,
                alias1 || st2.email as name
            from
                public.src_tbl1 as st1
                join
                    public.src_tbl2 as st2
                on
                    st1.id = st2.id
        ) as sq
    """
    assert_column_lineage_equal(
        sql,
        [
            (
                ColumnQualifierTuple("alias1", None),
                ColumnQualifierTuple("name", "public.tgt_tbl1"),
            ),
            (
                ColumnQualifierTuple("email", "public.src_tbl2"),
                ColumnQualifierTuple("name", "public.tgt_tbl1"),
            ),
        ],
        test_sqlparse=False,
    )
