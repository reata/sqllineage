**************
Why SQLLineage
**************

How It Starts
=============

Back when I was a data engineer, SQL is something people can't avoid in this industry. I guess it still is since you're
reading this. However popular, it doesn't change the fact the SQL code can be nested, verbose and very difficult to read.
Oftentimes, I found myself lost in thousands lines of SQL code full of seemingly invaluable business logic.
I have to dive deep to understand which tables this SQL script are reading, how they're joined together and the result
is writing to which table. That's really painful experience.

Later I became a software engineer, developing all kinds of platforms for data engineer to enhance their efficiency:
job orchestration system, metadata management system, to name a few. That's when the SQL Lineage puzzle found me again.
I want a table-level, even column-level lineage trace in my metadata management system. Dependency configuration
is tedious work in job orchestration. I want my data engineer colleagues to just write the SQL, feed it to my system,
and I'll show them the correct jobs they should depend on given their SQL code.

Back then, I wasn't equipped with the right tool though. Without much understanding of parsing, lexical analyser, let alone
a real compiler, regular expression was my weapon of choice. And the strategy is simple enough, whenever I see "from"
or "join", I extract the word after it as source table. Whenever I see "insert overwrite", a target table must follow
upon. I know you must have already figured out several pitfalls that comes with this approach. How about when "from" is
in a comment, or even when it is a string value used in where clause? These are all valid points, but somehow, I
managed to survive with this approach plus all kinds of if-else logic. Simple as it is, 80% of the time, I got it right.

But to be more accurate, even get it right 100% of the time, I don't think regular expression could do the trick. So I
searched the internet, tons of sql parsers, while no handy/user-friendly tool to use these parsers to analyze the SQL lineage,
not at least in Python world. So I thought, hey, why not build one for my own. That's how I decided to start this project.
It aims at bridging the gap between a) sql parser which explains the sql in a way that computer (sql engine) could understand,
and b) sql developers with the knowledge to write it while without technical know-how for how this sql code is actually
parsed, analyzed and executed.

**With SQLLineage, it's all just human-readable lineage result**.


Broader Use Cases
=================
SQL lineage, or data lineage if we include generic non-SQL jobs, is the jewel in the data engineering crown. A well maintained
lineage service can greatly ease the pains that different roles in a data team suffer from.

Here's a few classical use cases for lineage:

- For Data Producer
    - **Dependency Recommendation**: recommending dependency for jobs, detecting missing or unnecessary dependency
      to avoid potential issues.
    - **Impact analysis**: notifying downstream customer when data quality issue happens, tracing back to upstream for
      root cause analysis, understanding how much impact it would be when changing a table/column.
    - **Development Standard Enforcement**: detecting anti-pattern like one job producing multiple production tables, or
      temporary tables created without being accessed later.
    - **Job ETA Prediction and Alert**: predicting table delay and potential SLA miss with job running information.

- For Data Governor
    - **Table/Column Lifecycle**: identifying unused tables/columns, and retiring it.
    - **GDPR Compliance**: propagating the PII columns tag along the lineage path, easing the manually tagging process.
      Foundation for later PII encryption and GDPR deletion.

- For Data Consumer
    - **Understanding data flow**: discovering table, understanding the table from table flow perspective.
    - **Verify business logic**: making sure the information I use is sourcing from the correct dataset with correct
      transformation.
