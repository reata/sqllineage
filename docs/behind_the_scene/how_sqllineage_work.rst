************************
How Does SQLLineage Work
************************

Basically a sql parser will parse the SQL statement(s) into `AST`_ (Abstract Syntax Tree), which according to wikipedia,
is a tree representation of the abstract syntactic structure of source code (in our case, SQL code, of course). This is
where SQLLineage takes over.

With AST generated, SQLLineage will traverse through this tree, apply some pre-defined rules, so as to extract the part
we're interested in.


.. _AST: https://en.wikipedia.org/wiki/Abstract_syntax_tree
