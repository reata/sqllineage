******
Holder
******

LineageHolder is an abstraction to hold the lineage result analyzed by LineageAnalyzer at different level.

At the bottom, we have :class:`sqllineage.core.holders.SubQueryLineageHolder` to hold lineage at subquery level.
This is used internally by :class:`sqllineage.core.analyzer.LineageAnalyzer`.

LineageAnalyzer generates :class:`sqllineage.core.holder.StatementLineageHolder`
as the result of lineage at SQL statement level.

To assemble multiple :class:`sqllineage.core.holder.StatementLineageHolder` into a DAG based data structure serving
for the final output, we have :class:`sqllineage.core.holders.SQLLineageHolder`


SubQueryLineageHolder
==============================================

.. autoclass:: sqllineage.core.holders.SubQueryLineageHolder
    :members:


StatementLineageHolder
==============================================

.. autoclass:: sqllineage.core.holders.StatementLineageHolder
    :members:


SQLLineageHolder
==============================================

.. autoclass:: sqllineage.core.holders.SQLLineageHolder
    :members:
