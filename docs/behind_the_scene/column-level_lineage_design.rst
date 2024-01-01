***************************
Column-Level Lineage Design
***************************

Key Design Principles
=====================
* **sqllineage will stay primarily as a static code analysis tool**, so we must tolerate information missing when doing
  column-level lineage. Maybe somewhere in the future, we can provide some kind of plugin mechanism to register metadata
  as a supplement to refine the lineage result, but in no way will we depend solely on metadata.
* **Don't create column-level lineage DAG to be a separate graph from table-level DAG**. There should be one unified DAG.
  Either 1) we build a DAG to the granularity of column so with some kind of transformation, we can derive table-level
  DAG from it. To use an analogy of relational data, it's like building a detail table, with the ability to aggregate
  up to a summary table. Or 2) we build a property graph (see `JanusGraph docs`_ for reference), with Table and Column as
  two kinds of Vertex, and two type of edges, one for column to table relationship mapping, one for column-level as well
  as table-level lineage.

Questions Before Implementation
===============================

1. **What's the data structure to represent Column-Level Lineage?**
   Currently we're using ``DiGraph`` in library ``networkx`` to represent Table-Level Lineage, with table as vertex and
   table-level lineage as edge, which is pretty straight forward. After changing to Column-Level, what's the plan?

**Answer**: See design principle for two possible data structure. I would prefer property graph for this.

2. **How do we deal with** ``select *`` **?**
   In following case, we don't know which columns are in tab2

.. code-block:: sql

    INSERT OVERWRITE tab1
    SELECT * FROM tab2;

**Answer**: Add an virtual column * for each Table as a special column. So the lineage is tab2.* -> tab1.*

3. **How do we deal with column without table/alias prefix in case of join?**
   In following case, we don't know whether col2 is coming from tab2 or tab3

.. code-block:: sql

    INSERT OVERWRITE tab1
    SELECT col2
    FROM tab2
    JOIN tab3
    ON tab2.col1 = tab3.col1

**Answer**: Add two edges, tab2.col2 -> tab1.col2, tab3.col2 -> tab1.col2. Meanwhile, these two edges should be marked
so that later in visualization, they can be drawn differently, like in dot line.

Implementation Plan
===================

1. Atomic column logic handling: alias, case when, function, expression, etc.
2. Subquery recognition and lineage transition from subquery to statement
3. Column to table assignment in case of table join
4. Assemble Statement Level lineage into multiple statements DAG.


.. note::
    Column-Level lineage is now released with v1.3.0


.. _JanusGraph docs: https://docs.janusgraph.org/schema/
