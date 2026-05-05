import pytest

from ...helpers import assert_table_lineage_equal

proc_dialects = [
    "bigquery",
    "oracle",
    "tsql",
]


@pytest.mark.parametrize("dialect", proc_dialects)
def test_procedure_with_select(dialect: str):
    sql = f"""CREATE PROCEDURE proc1()
{'' if dialect == 'bigquery' else 'AS'}
BEGIN
SELECT col1 FROM tab1;
END"""
    assert_table_lineage_equal(
        sql, {"tab1"}, None, dialect=dialect, test_sqlparse=False
    )


@pytest.mark.parametrize("dialect", proc_dialects)
def test_procedure_with_insert(dialect: str):
    sql = f"""CREATE PROCEDURE proc1()
{'' if dialect == 'bigquery' else 'AS'}
BEGIN
INSERT INTO tab1 (col1) VALUES (1);
END"""
    assert_table_lineage_equal(
        sql, None, {"tab1"}, dialect=dialect, test_sqlparse=False
    )


@pytest.mark.parametrize("dialect", proc_dialects)
def test_procedure_with_select_and_insert(dialect: str):
    sql = f"""CREATE PROCEDURE proc1()
{'' if dialect == 'bigquery' else 'AS'}
BEGIN
INSERT INTO tab2 (col1)
SELECT col1 FROM tab1;
END"""
    assert_table_lineage_equal(
        sql,
        {"tab1"},
        {"tab2"},
        dialect=dialect,
        test_sqlparse=False,
    )


@pytest.mark.parametrize("dialect", proc_dialects)
def test_procedure_multiple_statements(dialect: str):
    sql = f"""CREATE PROCEDURE proc1()
{'' if dialect == 'bigquery' else 'AS'}
BEGIN
INSERT INTO tab2 (col1)
SELECT col1 FROM tab1;
INSERT INTO tab3 (col2)
SELECT col2 FROM tab2;
END"""
    assert_table_lineage_equal(
        sql,
        {"tab1", "tab2"},
        {"tab2", "tab3"},
        dialect,
        test_sqlparse=False,
    )
