*************
LineageRunner
*************

LineageRunner is the entry point for SQLLineage core processing logic. After parsing command-line options, a string
representation of SQL statements will be feed to LineageRunner for processing. It contains three steps in total:

1. Calling ``sqlparse.parse`` function to parse string-base SQL statements into a list of ``sqlparse.sql.Statement``

2. Calling :class:`sqllineage.core.LineageAnalyzer` to analyze each ``sqlparse.sql.Statement`` and get a list of
   :class:`sqllineage.core.LineageResult` .

3. Calling :class:`sqllineage.combiner.combine` function to combine the list of :class:`sqllineage.core.LineageResult`
   into :class:`sqllineage.combiners.CombinedLineageResult`.

:class:`sqllineage.combiners.CombinedLineageResult` then will server for lineage summary, in words or in visualization
form.

sqllineage.runner.LineageRunner
===============================

.. autoclass:: sqllineage.runner.LineageRunner
    :members:
    :special-members: __str__


sqllineage.runner.main
======================

.. autofunction:: sqllineage.runner.main
