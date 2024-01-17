import os
from pprint import pprint
from typing import Optional, Dict, List

import sqlfluff
from sqlfluff.core import Linter
from sqlfluff.core.parser import segments

from sqllineage.config import SQLLineageConfig
from sqllineage.runner import LineageRunner
from sqllineage.core.metadata_provider import MetaDataProvider


class v_hive(MetaDataProvider):
    def __init__(self, metadata: Optional[Dict[str, List[str]]] = None):
        super().__init__()
        self.metadata = metadata if metadata is not None else {}

    def _get_table_columns(self, schema: str, table: str, **kwargs) -> List[str]:
        return self.metadata.get(f"{schema}.{table}", [])


def go1():
    v_meta = {
        "ods.target": ["acct_month", "user_id", "user_name"],
        "ods.source": ["acct_month", "user_code", "nick_name"],
    }

    os.environ["SQLLINEAGE_DEFAULT_SCHEMA"] = "ods"
    v_sql = """
    INSERT INTO target 
WITH cte1 AS (SELECT acct_month as acct_id, user_code as xxx, nick_name as yyy FROM source)
SELECT acct_id, xxx, yyy FROM cte1 """
    parse = LineageRunner(v_sql, metadata_provider=v_hive(v_meta))
    print(parse.get_column_lineage())


def go2():
    from sqllineage.runner import LineageRunner

    v_sql = "insert into ta select b from (select b from tb union all select c from tc ) sub"
    parse = LineageRunner(sql=v_sql)
    for col_tuple in parse.get_column_lineage_noSubQuery():
        print(col_tuple)


def go3():
    v_sql = """INSERT INTO ods.target_tab
WITH cte1 AS (SELECT day_id as acct_id, user_id as xxx, user_name as yyy FROM ods.source_a)
SELECT acct_id, xxx, yyy FROM cte1
"""

    from sqllineage.runner import LineageRunner

    parse = LineageRunner(sql=v_sql.lower(), dialect="sparksql")
    parse.print_column_lineage()


def go4():
    from sqllineage.runner import LineageRunner
    from sqlfluff.core.linter import Linter

    v_sql = "insert into ta select '2023' dayid, id from tb "
    parse = LineageRunner(sql=v_sql)
    b = Linter(dialect="ansi").parse_string(v_sql).tree
    for col_tuple in parse.get_column_lineage(exclude_subquery=True):
        print(col_tuple)


def go5():
    v_sql = """INSERT INTO ods.target_tab (WITH tab1 AS (SELECT day_id as acct_id, user_id as xxx, user_name as yyy FROM ods.source_a) SELECT acct_id, xxx, yyy FROM tab1);"""

    # pprint(sqlfluff.parse(v_sql)['file']['statement'])
    # a=sqlfluff.parse(v_sql)['file']['statement']
    # for seg in sqlfluff.parse(v_sql)['file']['statement']['insert_statement']:
    #     print(seg)

    a = Linter(dialect="mysql").parse_string(v_sql)
    # print(a.tree.getattr(parsed.tree, "segments")

    # print(a.tree.segments[0].segments)
    a = a.tree.segments[0].segments[0]
    # print(a)

    is_start = False
    if a.segments[0].raw_upper == "INSERT":
        print("insert")
    for i, x in enumerate(a.segments):
        if x.type == "whitespace":
            continue
        if x.type == "table_reference":
            is_start = True
            continue

        if x.type == "bracketed" and is_start:
            print("no find ")
            for y in x.segments:
                print(y.type, y.raw)
            # c=set([x.type for x in x.segments])
            # print(c )
            b = set([x.type for x in x.segments]).difference(
                {"symbol", "indent", "whitespace", "column_reference", "dedent"}
            )
            print(b)
        # symbol indent whitespace column_reference dedent
        # print(i, x.type,x.__class__)
        # print(x)

    # parse = LineageRunner(sql=v_sql,dialect='sparksql')
    # for col_tuple in parse.get_column_lineage(exclude_subquery=True):
    #     print(col_tuple)


def go6():
    v_sql = """
    INSERT INTO ods.target_tab
WITH cte1 AS (SELECT day_id as acct_id, user_id as xxx, user_name as yyy FROM ods.source_a)
SELECT acct_id, xxx, yyy FROM cte1"""

    meta_collect = {
        "ods.source_a": ["day_id", "user_id", "user_name"],
        "ods.target_tab": ["day_no", "user_code", "name"],
    }
    parse = LineageRunner(v_sql, metadata_provider=v_hive(meta_collect))
    print(parse.get_column_lineage())


def go7():
    v_meta = {
        "ods.source_a": ["day_id", "user_id", "user_name"],
        "ods.target_tab": ["day_no", "user_code", "name"],
    }

    v_sql = """INSERT INTO ods.target_tab (WITH tab1 AS (SELECT day_id as acct_id, user_id as xxx, user_name as yyy FROM ods.source_a) SELECT acct_id, xxx, yyy FROM tab1);"""
    parse = LineageRunner(v_sql, metadata_provider=v_hive(v_meta))
    # print(parse.get_column_lineage(exclude_subquery=True))
    #
    parse.print_table_lineage()
    parse.print_column_lineage()

    # parse.print_table_lineage()


def go8():
    sql = """insert into target_tab select user_id, user_name from source_tab_1, source_tab_2"""
    parse = LineageRunner(sql, default_schema="ods")
    parse.print_table_lineage()
    # assert_table_lineage_equal(
    #     sql=sql,
    #     source_tables={"ods.source_tab_1"},
    #     target_tables={"ods.target_tab"},
    #     default_schema="ods",
    # )


def go9():
    os.environ["SQLLINEAGE_DEFAULT_SCHEMA"] = "ods"
    SQLLineageConfig.DEFAULT_SCHEMA = "ods"
    v_sql = """
        INSERT INTO target 
    
    SELECT acct_id, xxx, yyy FROM source """
    parse = LineageRunner(v_sql)
    parse.print_table_lineage()


if __name__ == "__main__":
    go9()

    # go5()
