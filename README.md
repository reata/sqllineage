# SQLLineage
SQL Lineage Analysis Tool powered by Python

[![image](https://img.shields.io/pypi/v/sqllineage.svg)](https://pypi.org/project/sqllineage/)
[![image](https://img.shields.io/pypi/status/sqllineage.svg)](https://pypi.org/project/sqllineage/)
[![image](https://img.shields.io/pypi/pyversions/sqllineage.svg)](https://pypi.org/project/sqllineage/)
[![image](https://img.shields.io/pypi/l/sqllineage.svg)](https://pypi.org/project/sqllineage/)
[![Build Status](https://travis-ci.org/reata/sqllineage.svg?branch=master)](https://travis-ci.org/reata/sqllineage)
[![Documentation Status](https://readthedocs.org/projects/sqllineage/badge/?version=latest)](https://sqllineage.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/reata/sqllineage/branch/master/graph/badge.svg)](https://codecov.io/gh/reata/sqllineage)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![security: bandit](https://img.shields.io/badge/security-bandit-yellow.svg)](https://github.com/PyCQA/bandit)

Never get the hang of a SQL parser? SQLLineage comes to the rescue. Given a SQL command, SQLLineage will tell you its
source and target tables, without worrying about Tokens, Keyword, Identifier and all the jagons used by SQL parsers.

Behind the scene, SQLLineage uses the fantastic [`sqlparse`](https://github.com/andialbrecht/sqlparse) library to parse 
the SQL command, and bring you all the human-readable result with ease.


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

Lineage result combined for multiple SQL statements, with intermediate tables identified:
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

And if you want to see lineage result for every SQL statement, just toggle verbose option
```
$ sqllineage -v -e "insert into db1.table1 select * from db2.table2; insert into db3.table3 select * from db1.table1;"
Statement #1: insert into db1.table1 select * from db2.table2;
    table read: {Table: db2.table2}
    table write: {Table: db1.table1}
    table rename: {}
    table drop: {}
    table intermediate: {}
Statement #2: insert into db3.table3 select * from db1.table1;
    table read: {Table: db1.table1}
    table write: {Table: db3.table3}
    table rename: {}
    table drop: {}
    table intermediate: {}
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

Or specify your own combiner to combine lineage result, here we use a naive combiner, which is just to union each table set without removing intermediate tables:
```
$ sqllineage -e "insert into db1.table1 select * from db2.table2; insert into db3.table3 select * from db1.table1;" -c sqllineage.combiners.NaiveLineageCombiner
Statements(#): 2
Source Tables:
    db2.table2
    db1.table1
Target Tables:
    db3.table3
    db1.table1
```
