# SQLLineage
SQL Lineage Analysis Tool powered by Python

[![image](https://img.shields.io/pypi/v/sqllineage.svg)](https://pypi.org/project/sqllineage/)
[![image](https://img.shields.io/pypi/status/sqllineage.svg)](https://pypi.org/project/sqllineage/)
[![image](https://img.shields.io/pypi/pyversions/sqllineage.svg)](https://pypi.org/project/sqllineage/)
[![image](https://img.shields.io/pypi/l/sqllineage.svg)](https://pypi.org/project/sqllineage/)
[![Build Status](https://travis-ci.org/reata/sqllineage.svg?branch=master)](https://travis-ci.org/reata/sqllineage)
[![Documentation Status](https://readthedocs.org/projects/sqllineage/badge/?version=latest)](https://sqllineage.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/reata/sqllineage/branch/master/graph/badge.svg)](https://codecov.io/gh/reata/sqllineage)

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
$ sqllineage -e "insert into table1 select * from table2"
Statements(#): 1
Source Tables:
    table2
Target Tables:
    table1
```

Or you can parse a SQL file with -f option:
```
$ sqllineage -f foo.sqlStatements(#): 1
Statements(#): 1
Source Tables:
    table_foo
    table_bar
Target Tables:
    table_baz
```
