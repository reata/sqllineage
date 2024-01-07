**************
Advanced Usage
**************

Multiple SQL Statements
=======================

Lineage is combined from multiple SQL statements, with intermediate tables identified

.. code-block:: bash

    $ sqllineage -e "insert into db1.table1 select * from db2.table2; insert into db3.table3 select * from db1.table1;"
    Statements(#): 2
    Source Tables:
        db2.table2
    Target Tables:
        db3.table3
    Intermediate Tables:
        db1.table1


Verbose Lineage Result
======================

And if you want to see lineage for each SQL statement, just toggle verbose option

.. code-block:: bash

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


Dialect-Awareness Lineage
=========================
By default, sqllineage use `ansi` dialect to parse and validate your SQL. However, some SQL syntax you take for granted
in daily life might not be in ANSI standard. In addition, different SQL dialects have different set of SQL keywords,
further weakening sqllineage's capabilities when keyword used as table name or column name. To get the most out of
sqllineage, we strongly encourage you to pass the dialect to assist the lineage analyzing.

Take below example, `INSERT OVERWRITE` statement is only supported by big data solutions like Hive/SparkSQL, and `MAP`
is a reserved keyword in Hive thus can not be used as table name while it is not for SparkSQL. Both ansi and hive dialect
tell you this causes syntax error and sparksql gives the correct result:

.. code-block:: bash

    $ sqllineage -e "insert overwrite table map select * from foo"
    ...
    sqllineage.exceptions.InvalidSyntaxException: This SQL statement is unparsable, please check potential syntax error for SQL

    $ sqllineage -e "insert overwrite table map select * from foo" --dialect=hive
    ...
    sqllineage.exceptions.InvalidSyntaxException: This SQL statement is unparsable, please check potential syntax error for SQL

    $ sqllineage -e "insert overwrite table map select * from foo" --dialect=sparksql
    Statements(#): 1
    Source Tables:
        <default>.foo
    Target Tables:
        <default>.map


Use `sqllineage \-\-dialects` to see all available dialects.


Column-Level Lineage
====================

We also support column level lineage in command line interface, set level option to column, all column lineage path
will be printed.

.. code-block:: sql

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


Suppose this sql is stored in a file called test.sql

.. code-block:: bash

    $ sqllineage -f test.sql -l column
    <default>.corge.col1 <- <default>.foo.col1 <- <default>.bar.col1
    <default>.corge.col2 <- <default>.foo.col2 <- <default>.baz.col1
    <default>.corge.col2 <- <default>.grault.col2
    <default>.foo.* <- <default>.quux.*
    <default>.foo.col3 <- c.col3_sum <- <default>.qux.col3
    <default>.foo.col4 <- col4


MetaData-Awareness Lineage
==========================

By observing the column lineage generated from previous step, you'll possibly notice that:

1. `<default>.foo.* <- <default>.quux.*`: the wildcard is not expanded.
2. `<default>.foo.col4 <- col4`: col4 is not assigned with source table.

It's not perfect because we don't know the columns encoded in `*` of table `quux`. Likewise, given the context,
col4 could be coming from `bar`, `baz` or `quux`. Without metadata, this is the best sqllineage can do.

User can optionally provide the metadata information to sqllineage to improve the lineage result.

Suppose all the tables are created in sqlite database with a file called `db.db`. In particular,
table `quux` has columns `col5` and `col6` and `baz` has column `col4`.

.. code-block:: bash

    sqlite3 db.db 'CREATE TABLE IF NOT EXISTS baz (bar_id int, col1 int, col4 int)';
    sqlite3 db.db 'CREATE TABLE IF NOT EXISTS quux (quux_id int, col5 int, col6 int)';

Now given the same SQL, column lineage is fully resolved.

.. code-block:: bash

    $ SQLLINEAGE_DEFAULT_SCHEMA=main sqllineage -f test.sql -l column --sqlalchemy_url=sqlite:///db.db
    main.corge.col1 <- main.foo.col1 <- main.bar.col1
    main.corge.col2 <- main.foo.col2 <- main.bar.col1
    main.corge.col2 <- main.grault.col2
    main.foo.col3 <- c.col3_sum <- main.qux.col3
    main.foo.col4 <- main.baz.col4
    main.foo.col5 <- main.quux.col5
    main.foo.col6 <- main.quux.col6

The default schema name in sqlite is called `main`, we have to specify here because the tables in SQL file are unqualified.

SQLLineage leverages `sqlalchemy`_ to retrieve metadata from different SQL databases.
Check for more details on SQLLineage `MetaData`_.


.. _sqlalchemy: https://github.com/sqlalchemy/sqlalchemy
.. _MetaData: https://sqllineage.readthedocs.io/en/latest/gear_up/metadata.html


Lineage Visualization
=====================

One more cool feature, if you want a graph visualization for the lineage result, toggle graph-visualization option

Still using the above SQL file:

.. code-block:: bash

    sqllineage -g -f test.sql

A webserver will be started, showing DAG representation of the lineage result in browser.

Table-Level Lineage:

.. image:: ../_static/table.jpg
   :alt: Table lineage visualization

Column-Level Lineage:

.. image:: ../_static/column.jpg
   :alt: Column lineage visualization
