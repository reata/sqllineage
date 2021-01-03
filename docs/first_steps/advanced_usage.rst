**************
Advanced Usage
**************

Multiple SQL Statements
=======================

Lineage result combined for multiple SQL statements, with intermediate tables identified

.. code-block:: bash

    $ sqllineage -e "insert into db1.table1 select * from db2.table2; insert into db3.table3 select * from db1.table1;"
    Statements(#): 2
    Source Tables:
        db2.table2
    Target Tables:
        db3.table3
    Intermediate Tables:
        db1.table1


Verbose Lineage Result
======================

And if you want to see lineage result for every SQL statement, just toggle verbose option

.. code-block:: bash

    $ sqllineage -v -e "insert into db1.table1 select * from db2.table2; insert into db3.table3 select * from db1.table1;"
    Statement #1: insert into db1.table1 select * from db2.table2;
        table read: [Table: db2.table2]
        table write: [Table: db1.table1]
        table rename: []
        table drop: []
        table intermediate: []
    Statement #2: insert into db3.table3 select * from db1.table1;
        table read: [Table: db1.table1]
        table write: [Table: db3.table3]
        table rename: []
        table drop: []
        table intermediate: []
    ==========
    Summary:
    Statements(#): 2
    Source Tables:
        db2.table2
    Target Tables:
        db3.table3
    Intermediate Tables:
        db1.table1


Lineage Visualization
=====================

One more cool feature, if you want a graph visualization for the lineage result, toggle graphviz option

.. code-block:: bash

    sqllineage -g -e "insert into db1.table11 select * from db2.table21 union select * from db2.table22; insert into db3.table3 select * from db1.table11 join db1.table12;"

A webserver will be started, showing DAG representation of the lineage result in browser:

.. image:: ../_static/Figure_1.png
   :alt: Lineage visualization

For visualization to work, you must have extra dependencies (flask) installed::

    pip install sqllineage[all]

