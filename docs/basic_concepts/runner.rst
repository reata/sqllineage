******
Runner
******

LineageRunner is the entry point for SQLLineage core processing logic. After parsing command-line options, a string
representation of SQL statements will be fed to LineageRunner for processing. From a bird's-eye view, it contains
three steps:

1. Calling ``sqllineage.utils.helpers.split`` function to split string-base SQL statements into a list of ``str`` statements.

2. Calling :class:`sqllineage.core.analyzer.LineageAnalyzer` to analyze each one statement sql string. Get a list of
   :class:`sqllineage.core.holders.StatementLineageHolder` .

3. Calling :class:`sqllineage.core.holders.SQLLineageHolder.of` function to assemble the list of
   :class:`sqllineage.core.holders.StatementLineageHolder` into one :class:`sqllineage.core.holders.SQLLineageHolder`.

:class:`sqllineage.core.holders.SQLLineageHolder` then will serve for lineage summary, in text or in visualization
form.

sqllineage.runner.LineageRunner
===============================

.. autoclass:: sqllineage.runner.LineageRunner
    :members:
    :special-members: __str__


sqllineage.cli.main
======================

.. autofunction:: sqllineage.cli.main
