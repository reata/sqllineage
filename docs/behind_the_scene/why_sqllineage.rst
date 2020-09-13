**************
Why SQLLineage
**************

When I was a data engineer, SQL is something people can't avoid in this business. However popular, it doesn't change
the fact the SQL code can be nested, verbose and very difficult to read. Oftentimes, I found myself lost in thousands
lines of SQL code full of seemingly invaluable business logic. I have to dive deep to understand which tables are this
SQL script read, how they're joined together and the result is writing to which table. That's painful experience.

Later I became more of a platform engineer, writing all kinds of tools/platforms for data engineer to enhance their
efficiency, among them were job orchestration system, metadata management system, to name a few. The SQL Lineage puzzle
found me again. I want a table-level, even column-level lineage trace in my metadata management system. Dependency
configuration is tedious work in job orchestration. I want my data engineer colleagues to just write the SQL,
feed it to my system, and I'll show them the rightful jobs they should depend on give their SQL code.

Back then, I wasn't equipped with the right tool though. Without much understanding of parsing, lexical analyser, let alone
the real compiler, regular expression was my weapon of choice. And the approach is simple enough, whenever I see "from"
or "join", I extract the word after it as source table. Whenever I see "insert overwrite", a target table must follow
behind it. I know you must already figure out the pitfalls that comes with this approach. How about when "from" is in a
comment, or even when it is a string value using in where condition? Any ways, I managed to survive with this approach,
plus with all kinds of if else logic. Simple as it is, 80% of the time, I got it right.

But to be more accurate, even get it right 100% of the time, I don't think regular expression could get me there. I
searched the internet, tons of sql parsers, while no handy/friendly tool to use these parsers to analyze the SQL lineage,
not at least in Python world. So I thought, hey, why not build one for my own. That's how I decided to write this
SQLLineage package. It aims at bridging the gap between sql parser and sql developer with the knowledge to write it but
without technical know-how as to how this sql code is actually parsed, analyzed and executed.

**With SQLLineage, it's all just human-readable lineage result**.
