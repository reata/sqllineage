***************
LineageAnalyzer
***************

LineageAnalyzer contains the core processing logic for one-statement SQL analysis.

At the core of analyzer is all kinds of ``sqllineage.core.handlers`` to handle the interested tokens and store the
result in ``sqllineage.core.holders``.

LineageAnalyzer
========================================

.. autoclass:: sqllineage.core.analyzer.LineageAnalyzer
    :members:


SourceHandler
========================================

.. autoclass:: sqllineage.core.handlers.source.SourceHandler


TargetHandler
========================================

.. autoclass:: sqllineage.core.handlers.target.TargetHandler


CTEHandler
========================================

.. autoclass:: sqllineage.core.handlers.cte.CTEHandler
