************************
How Does SQLLineage Work
************************

Basically a sql parser will parse the SQL statement(s) into `AST`_ (Abstract Syntax Tree), which according to wikipedia,
is a tree representation of the abstract syntactic structure of source code (in our case, SQL code, of course). This is
where SQLLineage takes over.

With AST generated, SQLLineage will traverse through this tree, apply some pre-defined rules, so as to extract the part
we're interested in.

`sqlparse`_ itself gives a simple `example`_ to extract table names, through which you can get a rough idea how
SQLLineage works. The core idea is when a token is Keyword and its value is "FROM", then the next token will either
be subquery or identifier. For subquery, we just recursively calling extract function. For identifier, there's a way
to get its value.

.. warning::
    Here is just a simplified description. In reality, we could easily see ``Comment`` coming after "FROM", or subquery
    mistakenly parsed as ``Identifier`` or ``Parenthesis``. These are all corner cases we should resolve in reality.

Some other simple rules in SQLLineage:

1. Things go after Keyword **"From"**, all kinds of **"JOIN"** will be source table.

2. Things go after Keyword **"INTO"**, **"OVERWRITE"**, **"TABLE"**, **"VIEW"** will be target table. (Though there are
   exceptions like drop table statement)

3. Things go after Keyword **"With"** will be intermediate table.

The rest thing is just tedious work. We collect all kinds of sql, handle various edge cases and make these simple rules
robust enough.

That's it for single statement SQLLineage. With multiple statements SQL, it requires some more extra work to assemble the
Lineage result from single statements.

We choose a `DAG`_ based data structure to represent multiple statements SQL lineage. Table/View will be vertex in this
graph while a edge means data in source vertex table will contribute to data in target vertex table. Every single
statement lineage result will contain table read and table write information, which will later be combined into this
graph. With this DAG based data structure, it is also very easy for lineage visualization.

.. _AST: https://en.wikipedia.org/wiki/Abstract_syntax_tree
.. _sqlparse: https://github.com/andialbrecht/sqlparse
.. _example: https://github.com/andialbrecht/sqlparse/blob/master/examples/extract_table_names.py
.. _DAG: https://en.wikipedia.org/wiki/Directed_acyclic_graph
