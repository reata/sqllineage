*******************
Beyond Command Line
*******************

Since sqllineage is a Python package, after installation, you can also import it and use the Python API to achieve
the same functionality.

.. code-block:: python

    >>> from sqllineage.runner import LineageRunner
    >>> sql = "insert into db1.table11 select * from db2.table21 union select * from db2.table22;"
    >>> sql += "insert into db3.table3 select * from db1.table11 join db1.table12;"
    >>> result = LineageRunner(sql)
    # To show lineage summary
    >>> print(result)
    Statements(#): 2
    Source Tables:
        db1.table12
        db2.table21
        db2.table22
    Target Tables:
        db3.table3
    Intermediate Tables:
        db1.table11
    # To parse all the source tables
    >>> for tbl in result.source_tables: print(tbl)
    db1.table12
    db2.table21
    db2.table22
    # likewise for target tables
    >>> for tbl in result.target_tables: print(tbl)
    db3.table13
    # To pop up a matplotlib visualization graph
    >>> result.draw()
