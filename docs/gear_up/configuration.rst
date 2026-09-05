*************
Configuration
*************

The SQLLineage configuration allows user to customize the behaviour of sqllineage.

We adopt environment variable approach for global key-value mapping. The keys listed in this section should start with
`"SQLLINEAGE_"` to be a valid config. For example, to use DEFAULT_SCHEMA, use ``SQLLINEAGE_DEFAULT_SCHEMA=default``.

.. note::
     Starting v1.5.2, we also support changing config at runtime. A local config is kept for each thread that will mask
     global config. Local configuration must be set using context manager:

     .. code-block:: python

        >>> from sqllineage.config import SQLLineageConfig
        >>> from sqllineage.runner import LineageRunner

        >>> with SQLLineageConfig(DEFAULT_SCHEMA="ods"):
        >>>     print(LineageRunner("select * from test").source_tables)
        [Table: ods.test]
        >>> with SQLLineageConfig(DEFAULT_SCHEMA="dwd"):
        >>>     print(LineageRunner("select * from test").source_tables)
        [Table: dwd.test]

     Note when setting local config, the key does not start with `"SQLLINEAGE_"`.

DEFAULT_SCHEMA
==============
Default schema, or interchangeably called database. Tables without schema qualifier parsed from SQL is set with a schema
named `<default>`, which represents an unknown schema name. If DEFAULT_SCHEMA is set, we will use this value as
default schema name.

Default: ``""``

Since: 1.5.0

DIRECTORY
=========
Frontend app SQL directory. By default the frontend app is showing the data directory packaged with sqllineage,
which includes tpcds queries for demo purposes. User can customize with this key.

Default: ``sqllineage/data``

Since: 1.2.1

LATERAL_COLUMN_ALIAS_REFERENCE
==============================
Enable lateral column alias reference. This is a syntax feature supported by some SQL dialects. See:

- Amazon Redshift: `Amazon Redshift announces support for lateral column alias reference`_
- Spark (since 3.4): `Support "lateral column alias references" to allow column aliases to be used within SELECT clauses`_
- Databricks: `Introducing the Support of Lateral Column Alias`_

.. note::
     Lateral column alias reference is a feature that must be used together with metadata for each column to be
     correctly resolved. Take below example:

     .. code-block:: sql

        SELECT clicks / impressions as probability,
               round(100 * probability, 1) as percentage
        FROM raw_data

     If table raw_data has a column named **probability**, **probability** in the second selected column is from table
     raw_data. Otherwise, it's referencing alias **clicks / impressions as probability**.

     That means with SQLLineage, besides making LATERAL_COLUMN_ALIAS_REFERENCE=TRUE, MetaDataProvider must also be
     provided so we can query raw_data table to see if it has a column named **probability** and then check alias reference.
     If not provided, we will fallback to default behavior to simply assume column **probability** is from table raw_data
     even if LATERAL_COLUMN_ALIAS_REFERENCE is set to TRUE.

Default: ``False``

Since: 1.5.1

TSQL_NO_SEMICOLON
=================
Enable tsql no semicolon splitter mode. Transact-SQL offers this feature that even when SQL statements are not delimited
by semicolon, it can still be parsed and executed.

.. warning::
     Quoting `Transact-SQL syntax conventions (Transact-SQL)`_, "although the semicolon isn't required for most
     statements in this version (v16) of SQL Server, it will be required in a future version".

     So with SQLLineage, this config key is kept mostly for backward-compatible purposes. We may drop the support any
     time without warning. Bear this in mind when using this feature.

Default: ``False``

Since: 1.4.8

GRAPH_OPERATOR_CLASS
====================
The graph operator is the pluggable layer that maintains the lineage graph. Two implementations are provided:

* ``sqllineage.core.graph.networkx.NetworkXGraphOperator``: the ``networkx``-based implementation, which is the default.
* ``sqllineage.core.graph.rustworkx.RustworkXGraphOperator``: the ``rustworkx``-based implementation, available since
  v1.5.7 as an opt-in choice.

While networkx remains the default, we recommend switching to rustworkx for better performance, in particular when
handling large lineage graphs. It has been validated against large-scale SQL workloads in production and shows
significant performance improvement over the networkx implementation while passing the whole test suite. Any import
errors when using a non-default graph operator will fallback to the networkx implementation.

.. note::
     rustworkx graph operator is still an opt-in feature: sqllineage defaults to the networkx implementation unless you
     opt in by setting this config, though we may switch the default in a future release. There might be edge cases
     not covered by our tests, so please report any issues you encounter when using rustworkx graph operator.

Default: ``sqllineage.core.graph.networkx.NetworkXGraphOperator``

Since: 1.5.7


.. _Amazon Redshift announces support for lateral column alias reference: https://aws.amazon.com/about-aws/whats-new/2018/08/amazon-redshift-announces-support-for-lateral-column-alias-reference/
.. _Support "lateral column alias references" to allow column aliases to be used within SELECT clauses: https://issues.apache.org/jira/browse/SPARK-27561
.. _Introducing the Support of Lateral Column Alias: https://www.databricks.com/blog/introducing-support-lateral-column-alias
.. _Transact-SQL syntax conventions (Transact-SQL): https://learn.microsoft.com/en-us/sql/t-sql/language-elements/transact-sql-syntax-conventions-transact-sql?view=sql-server-ver16
