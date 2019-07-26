***************
Getting Started
***************

Install via PyPI
==================

Install the package (or add it to your ``requirements.txt`` file):

.. code-block:: bash

    pip install sqllineage


SQLLineage in Command Line
=======================================

After installation, you will get a `sqllineage` command. It has two options:

    - -e option let you pass a quoted query string as SQL command
    - -f option let you pass a file that contains a or more SQL commands

.. code-block:: bash

    $ sqllineage -e "insert into table1 select * from table2"
    Statements(#): 1
    Source Tables:
        table2
    Target Tables:
        table1


.. code-block:: bash

    $ sqllineage -f foo.sql
    Statements(#): 1
    Source Tables:
        table_foo
        table_bar
    Target Tables:
        table_baz
