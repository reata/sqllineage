********************************
Dialect-Awareness Lineage Design
********************************

Problem Statement
=================
As of v1.3.x release, table level lineage is perfectly production-ready. Column level lineage, under the no-metadata
background, is also as good as it can be. And yet we still have a lot of corner cases that are not yet supported.
This is really due to the long-tail of SQL language features and fragmentation of various SQL dialects.

Here are some typical issues:

* How to check whether syntax is valid or not?

* dialect specific syntax:

  * MSSQL assignment operator
  * Snowflake MERGE statement
  * CURRENT_TIMESTAMP: keyword or function?
  * identifier quote character: double quote or backtick?

* dialect specific keywords:

  * reversed keyword vs non-reversed keyword list
  * non-reserved keyword as table name
  * non-reserved keyword as column name

* dialect specific function:

  * Presto UNNEST
  * Snowflake GENERATOR

Over the years, we already have several monkey patches and utils on sqlparse to tweak the AST generated, either because
of incorrect parsing result (e.g. parenthesized query followed by INSERT INTO table parsed as function) or not yet
supported token grouping (e.g. window function for example). Due to the non-validating nature of sqlparse, that's the
bitter pill to swallow when we enjoyed tons of convenience.

Wishful Thinking
================
To move forward, we'd want more from the parser so that:

1. We know better what syntax, or dialect specific feature we support.
2. We can easily revise parsing rules to generate the AST we want when we decide to support some new features.
3. User can specify the dialect when they use sqllineage, so they know what to expect. And we explicitly let them know
   when we don't know how to parse the SQL (InvalidSyntaxException) or how to analyze the lineage (UnsupportedStatementException).

Sample call from command line:

.. code-block:: bash

    sqllineage -f test.sql --dialect=ansi

Sample call from Python API:

.. code-block:: python

    from sqllineage.runner import LineageRunner
    sql = "select * from dual"
    result = LineageRunner(sql, dialect="ansi")

Likewise in frontend UI, user have a dropdown select to choose the dialect they want.

Implementation Plan
===================
`OpenMetadata`_ community contributed an implementation using the parser underneath sqlfluff. With `#326`_ merged into
master, we have a new `dialect` option. When passed with real dialect, like mysql, oracle, hive, sparksql, bigquery,
snowflake, etc, we'll leverage sqlfluff to analyze the query. A pseudo dialect `non-validating` is introduced to remain
backward compatibility, falling back to use sqlparse as parser.

We're running dual test using both parser and make sure the lineage result is exactly the same for every test case
(except for a few edge cases).

From code structure perspective, we refactored the whole code base to introduce a parser interface:

* LineageAnalyzer now accepts single statement SQL string, split by LineageRunner, and returns StatementLineageHolder
  as before
* Each parser implementations sit in folder **sqllineage.core.parser**. They're extending the LineageAnalyzer, common
  Models, and leverage Holders at different layers.

.. note::
    Dialect-awareness lineage is now released with v1.4.0

.. _OpenMetadata: https://open-metadata.org/
.. _#326: https://github.com/reata/sqllineage/pull/326
