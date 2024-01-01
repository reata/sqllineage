# SQLLineage
SQL Lineage Analysis Tool powered by Python

[![image](https://img.shields.io/pypi/v/sqllineage.svg)](https://pypi.org/project/sqllineage/)
[![image](https://img.shields.io/pypi/status/sqllineage.svg)](https://pypi.org/project/sqllineage/)
[![image](https://img.shields.io/pypi/pyversions/sqllineage.svg)](https://pypi.org/project/sqllineage/)
[![image](https://img.shields.io/pypi/l/sqllineage.svg)](https://pypi.org/project/sqllineage/)
[![Build Status](https://github.com/reata/sqllineage/workflows/build/badge.svg)](https://github.com/reata/sqllineage/actions)
[![Documentation Status](https://readthedocs.org/projects/sqllineage/badge/?version=latest)](https://sqllineage.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/reata/sqllineage/branch/master/graph/badge.svg)](https://codecov.io/gh/reata/sqllineage)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![security: bandit](https://img.shields.io/badge/security-bandit-yellow.svg)](https://github.com/PyCQA/bandit)

Never get the hang of a SQL parser? SQLLineage comes to the rescue. Given a SQL command, SQLLineage will tell you its
source and target tables, without worrying about Tokens, Keyword, Identifier and all the jagons used by SQL parsers.

Behind the scene, SQLLineage pluggable leverages parser library ([`sqlfluff`](https://github.com/sqlfluff/sqlfluff) 
and [`sqlparse`](https://github.com/andialbrecht/sqlparse)) to parse the SQL command, analyze the AST, stores the lineage
information in a graph (using graph library [`networkx`](https://github.com/networkx/networkx)), and brings you all the 
human-readable result with ease.

## Demo & Documentation
Talk is cheap, show me a [demo](https://reata.github.io/sqllineage/).

[Documentation](https://sqllineage.readthedocs.io) is online hosted by readthedocs, and you can check the 
[release note](https://sqllineage.readthedocs.io/en/latest/release_note/changelog.html) there.


## Quick Start
Install sqllineage via PyPI:
```bash
$ pip install sqllineage
```

Using sqllineage command to parse a quoted-query-string:
```
$ sqllineage -e "insert into db1.table1 select * from db2.table2"
Statements(#): 1
Source Tables:
    db2.table2
Target Tables:
    db1.table1
```

Or you can parse a SQL file with -f option:
```
$ sqllineage -f foo.sql
Statements(#): 1
Source Tables:
    db1.table_foo
    db1.table_bar
Target Tables:
    db2.table_baz
```

## Advanced Usage

### Multiple SQL Statements
Lineage is combined from multiple SQL statements, with intermediate tables identified:
```
$ sqllineage -e "insert into db1.table1 select * from db2.table2; insert into db3.table3 select * from db1.table1;"
Statements(#): 2
Source Tables:
    db2.table2
Target Tables:
    db3.table3
Intermediate Tables:
    db1.table1
```

### Verbose Lineage Result
And if you want to see lineage for each SQL statement, just toggle verbose option
```
$ sqllineage -v -e "insert into db1.table1 select * from db2.table2; insert into db3.table3 select * from db1.table1;"
Statement #1: insert into db1.table1 select * from db2.table2;
    table read: [Table: db2.table2]
    table write: [Table: db1.table1]
    table cte: []
    table rename: []
    table drop: []
Statement #2: insert into db3.table3 select * from db1.table1;
    table read: [Table: db1.table1]
    table write: [Table: db3.table3]
    table cte: []
    table rename: []
    table drop: []
==========
Summary:
Statements(#): 2
Source Tables:
    db2.table2
Target Tables:
    db3.table3
Intermediate Tables:
    db1.table1
```

### Dialect-Awareness Lineage
By default, sqllineage use `ansi` dialect to validate and parse your SQL. However, some SQL syntax you take for granted
in daily life might not be in ANSI standard. In addition, different SQL dialects have different set of SQL keywords,
further weakening sqllineage's capabilities when keyword used as table name or column name. To get the most out of
sqllineage, we strongly encourage you to pass the dialect to assist the lineage analyzing.

Take below example, `INSERT OVERWRITE` statement is only supported by big data solutions like Hive/SparkSQL, and `MAP`
is a reserved keyword in Hive thus can not be used as table name while it is not for SparkSQL. Both ansi and hive dialect
tell you this causes syntax error and sparksql gives the correct result:
```
$ sqllineage -e "INSERT OVERWRITE TABLE map SELECT * FROM foo"
...
sqllineage.exceptions.InvalidSyntaxException: This SQL statement is unparsable, please check potential syntax error for SQL

$ sqllineage -e "INSERT OVERWRITE TABLE map SELECT * FROM foo" --dialect=hive
...
sqllineage.exceptions.InvalidSyntaxException: This SQL statement is unparsable, please check potential syntax error for SQL

$ sqllineage -e "INSERT OVERWRITE TABLE map SELECT * FROM foo" --dialect=sparksql
Statements(#): 1
Source Tables:
    <default>.foo
Target Tables:
    <default>.map
```

Use `sqllineage --dialects` to see all available dialects.

### Column-Level Lineage
We also support column level lineage in command line interface, set level option to column, all column lineage path will 
be printed.

```sql
INSERT INTO foo
SELECT a.col1,
       b.col1     AS col2,
       c.col3_sum AS col3,
       col4,
       d.*
FROM bar a
         JOIN baz b
              ON a.id = b.bar_id
         LEFT JOIN (SELECT bar_id, sum(col3) AS col3_sum
                    FROM qux
                    GROUP BY bar_id) c
                   ON a.id = sq.bar_id
         CROSS JOIN quux d;

INSERT INTO corge
SELECT a.col1,
       a.col2 + b.col2 AS col2
FROM foo a
         LEFT JOIN grault b
              ON a.col1 = b.col1;
```

Suppose this sql is stored in a file called test.sql

```
$ sqllineage -f test.sql -l column
<default>.corge.col1 <- <default>.foo.col1 <- <default>.bar.col1
<default>.corge.col2 <- <default>.foo.col2 <- <default>.baz.col1
<default>.corge.col2 <- <default>.grault.col2
<default>.foo.* <- <default>.quux.*
<default>.foo.col3 <- c.col3_sum <- <default>.qux.col3
<default>.foo.col4 <- col4
```

### Lineage Visualization
One more cool feature, if you want a graph visualization for the lineage result, toggle graph-visualization option

Still using the above SQL file
```
sqllineage -g -f foo.sql
```
A webserver will be started, showing DAG representation of the lineage result in browser:

- Table-Level Lineage

<img src="https://raw.githubusercontent.com/reata/sqllineage/master/docs/_static/table.jpg" alt="Table-Level Lineage">

- Column-Level Lineage

<img src="https://raw.githubusercontent.com/reata/sqllineage/master/docs/_static/column.jpg" alt="Column-Level Lineage">
