**************
Why SQLLineage
**************

Back when I was a data engineer, SQL is something people can't avoid in this industry. I guess it still is since you're
reading this. However popular, it doesn't change the fact the SQL code can be nested, verbose and very difficult to read.
Oftentimes, I found myself lost in thousands lines of SQL code full of seemingly invaluable business logic.
I have to dive deep to understand which tables this SQL script are reading, how they're joined together and the result
is writing to which table. That's really painful experience.

Later I became more of a platform engineer, writing all kinds of tools/platforms for data engineer to enhance their
efficiency: job orchestration system, metadata management system, to name a few. That's when the SQL Lineage puzzle
found me again. I want a table-level, even column-level lineage trace in my metadata management system. Dependency
configuration is tedious work in job orchestration. I want my data engineer colleagues to just write the SQL,
feed it to my system, and I'll show them the correct jobs they should depend on given their SQL code.

Back then, I wasn't equipped with the right tool though. Without much understanding of parsing, lexical analyser, let alone
a real compiler, regular expression was my weapon of choice. And the strategy is simple enough, whenever I see "from"
or "join", I extract the word after it as source table. Whenever I see "insert overwrite", a target table must follow
upon. I know you must have already figured out several pitfalls that comes with this approach. How about when "from" is
in a comment, or even when it is a string value used in where condition? These are all valid points, but somehow, I
managed to survive with this approach plus all kinds of if-else logic. Simple as it is, 80% of the time, I got it right.

But to be more accurate, even get it right 100% of the time, I don't think regular expression could do the trick. So I
searched the internet, tons of sql parsers, while no handy/friendly tool to use these parsers to analyze the SQL lineage,
not at least in Python world. So I thought, hey, why not build one for my own. That's how I decided to start this project.
It aims at bridging the gap between a) sql parser which explains the sql in a way that computer (sql engine) could understand,
and b) sql developers with the knowledge to write it while without technical know-how for how this sql code is actually
parsed, analyzed and executed.

**With SQLLineage, it's all just human-readable lineage result**.
