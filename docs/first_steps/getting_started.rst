***************
Getting Started
***************

Install via PyPI
==================

Install the package via ``pip`` (or add it to your ``requirements.txt`` file), run::

   pip install sqllineage

If visualization feature is needed::

    pip install sqllineage[all]

Install via Github
==================

If you want the latest development version, you can install directly from Github::

    pip install git+https://github.com/reata/sqllineage.git


SQLLineage in Command Line
=======================================

After installation, you will get a `sqllineage` command. It has two major options:

    - -e option let you pass a quoted query string as SQL command
    - -f option let you pass a file that contains one or more SQL commands

.. code-block:: bash

    $ sqllineage -e "insert into table_foo select * from table_bar union select * from table_baz"
    Statements(#): 1
    Source Tables:
        <default>.table_bar
        <default>.table_baz
    Target Tables:
        <default>.table_foo


.. code-block:: bash

    $ sqllineage -f foo.sql
    Statements(#): 1
    Source Tables:
        <default>.table_bar
        <default>.table_baz
    Target Tables:
        <default>.table_foo
