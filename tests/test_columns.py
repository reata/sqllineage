from sqllineage.runner import LineageRunner

"""
This test class will contain all the tests for queries testing 'Column Lineage' where the dialect is not ANSI.
"""


def test_invalid_syntax_as_without_alias():
    sql = """INSERT OVERWRITE TABLE tab1
SELECT col1,
       col2 as,
       col3
FROM tab2"""
    # just assure no exception, don't guarantee the result
    LineageRunner(sql).print_column_lineage()
