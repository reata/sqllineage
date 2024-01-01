***************
Getting Started
***************

Install via PyPI
==================

Install the package via ``pip`` (or add it to your ``requirements.txt`` file), run::

   pip install sqllineage


Install via GitHub
==================

If you want the latest development version, you can install directly from GitHub::

    pip install git+https://github.com/reata/sqllineage.git


.. note::
    Installation from GitHub (or source code) requires **NodeJS/npm** for frontend code building, while for PyPI,
    we already pre-built the frontend code so Python/pip will be enough.

SQLLineage in Command Line
=======================================

After installation, you will get a `sqllineage` command. It has two major options:

    - -e option let you pass a quoted query string as SQL statements
    - -f option let you pass a file that contains SQL statements

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
