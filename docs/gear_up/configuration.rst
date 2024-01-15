*************
Configuration
*************

The SQLLineage configuration allows user to customize the behaviour of sqllineage.

We adopt environment variable approach for global key-value mapping. The keys listed in this section should start with
`"SQLLINEAGE_"` to be a valid config. For example, to use DEFAULT_SCHEMA, use ``SQLLINEAGE_DEFAULT_SCHEMA=default``.

DEFAULT_SCHEMA
==============
Default schema, or interchangeably called database. Tables without schema qualifier parsed from SQL is set with a schema
named `<default>`, which represents an unknown schema name. If DEFAULT_SCHEMA is set, we will use this value as
default schema name.

Default: ``""``

DIRECTORY
=========
Frontend app SQL directory. By default the frontend app is showing the data directory packaged with sqllineage,
which includes tpcds queries for demo purposes. User can customize with this key.

Default: ``sqllineage/data``

TSQL_NO_SEMICOLON
=================
Enable tsql no semicolon splitter mode.

.. warning::
     Transact-SQL offers this feature that even when SQL statements are not delimited by semicolon, it can still be
     parsed and executed. But quoting `tsql_syntax_convention`_, "although the semicolon isn't required for most
     statements in this version (v16) of SQL Server, it will be required in a future version". So this config key is
     kept mostly for backward-compatible purposes. We may drop the support any time without warning. Bear this in mind
     when using this feature with sqllineage.

Default: ``False``


.. _tsql_syntax_convention: https://learn.microsoft.com/en-us/sql/t-sql/language-elements/transact-sql-syntax-conventions-transact-sql?view=sql-server-ver16
