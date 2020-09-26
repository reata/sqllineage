***************
LineageCombiner
***************

Actually there's no such thing as LineageCombiner, only a combine function to do the trick of combining multiple
:class:`sqllineage.core.LineageResult` into a DAG based :class:`sqllineage.combiners.CombinedLineageResult`, which will server for
the final output.


sqllineage.combiners.combine
============================

.. autofunction:: sqllineage.combiners.combine


sqllineage.combiners.CombinedLineageResult
==========================================

.. autoclass:: sqllineage.combiners.CombinedLineageResult
    :members:
