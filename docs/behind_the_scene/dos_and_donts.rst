*************
DOs and DONTs
*************

SQLLineage is a static SQL code lineage analysis tool, which means we will not try to execute the SQL code against any
kinds of server. Instead, we will just look at the code as text, parse it and give back the result. No client/server
interaction.

DOs
===
* SQLLineage will continue to support most commonly used SQL system. Make best effort to be compatible.
* SQLLineage will stay mainly as a command line tool, as well as a Python utils library.

DONTs
=====
* Column-level lineage will not be 100% accurate because that would require metadata information. It's optional for user
  to leverage MetaDataProvider functionality so sqllineage can query metadata when analyzing. If not provided,
  column-to-table resolution will be conducted in a best-effort way, meaning we only provide possible table candidates
  for situation like ``select *`` or ``select col from tab1 join tab2``.

Static Code Analysis Approach Explained
=======================================

This is the typical flow of how SQL is parsed and executed from compiling perspective:

1. Lexical Analysis: transform SQL string into token stream.
2. Parsing: transform token stream into unresolved AST.
3. Semantic Analysis: annotate unresolved AST with real table/column reference using database catalog.
4. Optimize with Intermediate Representation: transform AST to execution plan, and optimize with predicate pushdown,
   column pruning, boolean simplification, limit combination, join strategy, etc.
5. Code Gen: This only makes sense in distributed SQL system. Generating primitive on underlying computing framework,
   like MapReduce Job, or RDD operation, based on the optimized execution plan.

.. note::
    Semantic analysis is a compiler term. In data world, it's often referred to as catalog resolution. For some systems,
    unresolved AST is transformed to unsolved execution plan first. With catalog resolution, a resolved execution plan
    is then ready for later optimization phases.

SQLLineage is working on the abstraction layer of unresolved AST, right after parsing phase is done. The good side is that
SQLLineage is dialect-agnostic and able to function without database catalog. The bad side is of course the inaccuracy
on column-level lineage when we don't know what's behind ``select *``.

The alternative way is starting the lineage analysis on the abstraction layer of resolved AST, or execution plan. That
ties lineage analysis tightly with the SQL system so it won't function without a live connection to database. But that will
give user an accurate result and the source code of database can be used to save a lot of coding effort.

To combine the good side of both approaches, SQLLineage introduces an optional MetaDataProvider, where user can register
metadata information in a programmatic way to assist column-to-table resolution.
