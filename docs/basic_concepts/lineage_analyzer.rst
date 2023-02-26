***************
LineageAnalyzer
***************

LineageAnalyzer is an abstract class, supposed to include the core processing logic for one-statement SQL analysis.

Each parser implementation will inherit LineageAnalyzer and do parser specific analysis based on the AST they generates
and store the result in ``sqllineage.core.holders``.

LineageAnalyzer
========================================

.. autoclass:: sqllineage.core.analyzer.LineageAnalyzer
    :members:
