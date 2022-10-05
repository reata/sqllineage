************************
How Does SQLLineage Work
************************

Basically a sql parser will parse the SQL statement(s) into `AST`_ (Abstract Syntax Tree), which according to wikipedia,
is a tree representation of the abstract syntactic structure of source code (in our case, SQL code, of course). This is
where SQLLineage takes over.

With AST generated, SQLLineage will traverse through this tree, apply some pre-defined rules, so as to extract the part
we're interested in. With that being said, SQLLineage is an AST application, while there's actually more you can do with
AST:

- **born duty of AST: the starting point for optimization.** In compiler world, machine code,
  or optionally IR (Intermediate Representation), will be generated based on the AST, and then code optimization,
  resulting in an optimized machine code. In data world, it's basically the same thing with different words,
  and different optimization target. AST will be converted to query execution plan for query execution optimization,
  using strategy like RBO(Rule Based Optimization) or CBO(Cost Based Optimization), so that database/data warehouse
  query engine can have an optimized physical plan for execution.

- **linter**: quoting wikipedia, `linter`_ is a static code analysis tool used to flag programming errors, bugs,
  stylistic errors and suspicious constructs. Oftentimes it's used interchangeably with a code formatter. Famous tools
  like flake8 for Python, ESLint for JavaScript. Golang even provide an official gofmt program in their standard library.
  Meanwhile, although not yet widely adopted in data world, we can also lint SQL code. `sqlfluff`_ is such an great tool.
  Guess how it works to detect a smelly "`SELECT *`" or a mixture of leading and trailing commas. The answer is AST!

- **transpiler**: This use case is most famous in JavaScript world, where they're proactively using syntax defined in
  latest language specification which is not supported by mainstream web browsers yet. Quote from its offical document,
  `Babel`_ is capable of "converting ECMAScript 2015+ code into a backwards compatible version of JavaScript in current
  and older browsers". Babel exists in frontend because there's less control over runtime choice compared to backend
  developers, you need both browser to support it and user to upgrade their browser. In data world, we also see similar
  requirements for SQL syntax difference between different SQL dialect. `sqlglot`_ is such an project, which can help
  you "translate" for example SQL written in Hive to Presto. All there are not possible without AST.

- **structure analysis**: IDE leverages this a lot. Scenarios like duplicate code detection, code refactor. Basically
  this is to analyze the code structure. SQLLineage also falls into this category.

`sqlparse`_ is the underlying parser SQLLineage uses to get the AST. It gives a simple `example`_ to extract table names,
through which you can get a rough idea of how SQLLineage works. At the core is when a token is Keyword and its value is
"FROM", then the next token will either be subquery or table. For subquery, we just recursively calling extract function.
For table, there's a way to get its name.

.. warning::
    This is just an over-simplified explanation. In reality, we could easily see ``Comment`` coming after "FROM", or
    subquery without alias (valid syntax in certain SQL dialect) mistakenly parsed as ``Parenthesis``. These are all
    corner cases we should resolve in real world.

.. note::
    Strictly speaking, sqlparse is generating a parse tree instead of an abstract syntax tree. There two terms are often
    used interchangeably, and indeed they're similar conceptually. They're both tree structure in slightly different
    abstraction layer. In the AST, information like comments and grouping symbols (parenthesis) are not represented.
    Removing comment doesn't change the code logic and parenthesis are already implicitly represented by the tree structure.

Some other simple rules in SQLLineage:

1. Things go after Keyword **"FROM"**, all kinds of **"JOIN"** will be source table.

2. Things go after Keyword **"INTO"**, **"OVERWRITE"**, **"TABLE"**, **"VIEW"** will be target table. (Though there are
   exceptions like drop table statement)

3. Things go after Keyword **"With"** will be CTE (Common Table Expression).

4. Things go after Keyword **"SELECT"** will be column(s).

The rest thing is just tedious work. We collect all kinds of sql, handle various edge cases and make these simple rules
robust enough.

That's it for single statement SQL lineage analysis. For multiple statements SQL, it requires some more extra work to
assemble the lineage from single statements.

We choose a `DAG`_ based data structure to represent multiple statements SQL lineage. Table/View will be vertex in this
graph while a edge means data in source vertex table will contribute to data in target vertex table. In column-level
lineage, the vertex is a column. Every single statement lineage result will contain table/column read and table/column
write information, which will later be combined into this graph. With this DAG based data structure, it is also very
easy to visualize lineage.

.. _AST: https://en.wikipedia.org/wiki/Abstract_syntax_tree
.. _linter: https://en.wikipedia.org/wiki/Lint_(software)
.. _sqlfluff: https://github.com/sqlfluff/sqlfluff
.. _Babel: https://babeljs.io/
.. _sqlglot: https://github.com/tobymao/sqlglot
.. _sqlparse: https://github.com/andialbrecht/sqlparse
.. _example: https://github.com/andialbrecht/sqlparse/blob/master/examples/extract_table_names.py
.. _DAG: https://en.wikipedia.org/wiki/Directed_acyclic_graph
