*********
Changelog
*********

v1.5.4
======
:Date: Feb 8, 2025

In this release, we deprecated Python 3.8 and added support for Python 3.13 based on Python end-of-life schedule.

The most notable enhancement is upgrading sqlfluff to 3.3.1. As a result, we can now support Impala dialect and
StarRocks dialect. Many parser issues are also fixed automatically by sqlfluff upgrade.

Enhancement
-------------
* Add Support for Python 3.13 (`#682 <https://github.com/reata/sqllineage/issues/682>`_)
* Drop Support for Python 3.8 (`#677 <https://github.com/reata/sqllineage/issues/677>`_)
* Support for Impala Dialect (`#645 <https://github.com/reata/sqllineage/issues/645>`_)
* Upgrade sqlfluff dependency to 3.3.1 (`#644 <https://github.com/reata/sqllineage/issues/644>`_)
* support Doris or StarRocks (`#498 <https://github.com/reata/sqllineage/issues/498>`_)

Bugfix
-------------
* Inconsistent order of lineage tuples (`#652 <https://github.com/reata/sqllineage/issues/652>`_)
* Metadata wrongly used in INSERT INTO statement column lineage (`#648 <https://github.com/reata/sqllineage/issues/648>`_)
* clickhouse <rename> not handled (`#642 <https://github.com/reata/sqllineage/issues/642>`_)
* Not read SQLfluff nested configs (`#628 <https://github.com/reata/sqllineage/issues/628>`_)
* False negative for Scalar Subquery used in Function (`#614 <https://github.com/reata/sqllineage/issues/614>`_)
* Column level lineage not drawn properly when metadata is provided (`#597 <https://github.com/reata/sqllineage/issues/597>`_)
* Tsql table names with square brackets are not resolved correctly (`#583 <https://github.com/reata/sqllineage/issues/583>`_)

v1.5.3
======
:Date: May 5, 2024

This is a security release to upgrade some dependencies to latest version as we receive vulnerability alerts.
We strongly recommend that all sqllineage installations be upgraded to this version immediately.

This release also includes an improvement regarding metadata provider.

Enhancement
-------------
* set target table column name from MetaDataProvider (`#528 <https://github.com/reata/sqllineage/issues/528>`_)

v1.5.2
======
:Date: April 7, 2024

Enhancement
-------------
* Enable support of sqlfluff context (`#548 <https://github.com/reata/sqllineage/issues/548>`_)
* Support Change Configure Default Schema At Runtime (`#536 <https://github.com/reata/sqllineage/issues/536>`_)

Bugfix
-------------
* Parse column level lineage incorrect (`#584 <https://github.com/reata/sqllineage/issues/584>`_)
* Metadata Masked When Table was in a previous UPDATE statement (`#577 <https://github.com/reata/sqllineage/issues/577>`_)
* Clickhouse SQL 'GLOBAL IN' not support (`#554 <https://github.com/reata/sqllineage/issues/554>`_)
* Support json_tuple in SELECT clause in Hive (`#483 <https://github.com/reata/sqllineage/issues/483>`_)

v1.5.1
======
:Date: February 4, 2024

This is a bugfix release mostly driven by community contributors. Thanks everyone for making SQLLineage better.

Enhancement
-------------
* Allow unambiguous column reference for JOIN with USING clause (`#558 <https://github.com/reata/sqllineage/issues/558>`_)
* Make Lateral Column Alias Reference Configurable (`#539 <https://github.com/reata/sqllineage/issues/539>`_)
* Add an Argument to Exclude SubQuery Column Node in Column Lineage Path (`#526 <https://github.com/reata/sqllineage/issues/526>`_)

Bugfix
-------------
* Not fully processed top-level subquery in DML (`#564 <https://github.com/reata/sqllineage/issues/564>`_)
* Missing target table with tsql parsing into statements with union (`#562 <https://github.com/reata/sqllineage/issues/562>`_)
* The second and subsequent case when subqueries in the select_clause are not correctly recognized (`#559 <https://github.com/reata/sqllineage/issues/559>`_)
* SQLLineageConfig boolean value returns True for all non-empty strings (`#551 <https://github.com/reata/sqllineage/issues/551>`_)
* Column lineage does not traverse through CTE containing uppercase letters (`#531 <https://github.com/reata/sqllineage/issues/531>`_)

v1.5.0
======
:Date: January 7, 2024

Great thanks to liznzn for contributing on MetaData-awareness lineage. Now we're able to generate more accurate
column lineage result for `select *` or select unqualified columns in case of table join through a unified
MetaDataProvider interface.

Also a breaking change is made to make ansi the default dialect in v1.5.x release as we target ultimately deprecating
non-validating dialect in v1.6.x release.

Breaking Change
---------------
* Make ansi the Default Dialect (`#518 <https://github.com/reata/sqllineage/issues/518>`_)

Feature
-------------
* Metadata Provider to Assist Column Lineage Analysis (`#477 <https://github.com/reata/sqllineage/issues/302>`_)

Enhancement
-------------
* Add a Configuration for Default Schema (`#523 <https://github.com/reata/sqllineage/issues/523>`_)
* Silent Mode Option to Suppress UnsupportedStatementException (`#513 <https://github.com/reata/sqllineage/issues/513>`_)
* Support Lateral Column Alias Reference Analyzing (`#507 <https://github.com/reata/sqllineage/issues/507>`_)
* Skip Lineage Analysis for SparkSQL Function Related Statement (`#500 <https://github.com/reata/sqllineage/issues/500>`_)
* update statement column lineage (`#487 <https://github.com/reata/sqllineage/issues/487>`_)

Bugfix
-------------
* subquery mistake alias as table name in visualization (`#512 <https://github.com/reata/sqllineage/issues/512>`_)
* InvalidSyntaxException When SQL Statement Ends with Multiple Semicolons (`#502 <https://github.com/reata/sqllineage/issues/502>`_)
* Misidentify Binary Operator * As Wildcard (`#485 <https://github.com/reata/sqllineage/issues/485>`_)
* adding type cast operator produces different results for redshift dialect (`#455 <https://github.com/reata/sqllineage/issues/455>`_)

v1.4.9
======
:Date: December 10, 2023

This is a bugfix release where we closed a bunch of issues concerning CTE and UNION

Bugfix
-------------
* Not Using Column Name Specified in Query For CTE within Query (`#486 <https://github.com/reata/sqllineage/issues/486>`_)
* CTE (Common Table Expressions) within CTE (`#484 <https://github.com/reata/sqllineage/issues/484>`_)
* lineage inaccurate when CTE used in subquery (`#476 <https://github.com/reata/sqllineage/issues/476>`_)
* UNION ALL Queries resolves column lineage incorrectly (`#475 <https://github.com/reata/sqllineage/issues/475>`_)
* Missing table when parsing sql with UNION ALL (`#466 <https://github.com/reata/sqllineage/issues/466>`_)
* No target tables in UPDATE statement using CTE (`#453 <https://github.com/reata/sqllineage/issues/453>`_)

v1.4.8
======
:Date: October 16, 2023

Enhancement
-------------
* Support Python 3.12 (`#469 <https://github.com/reata/sqllineage/issues/469>`_)
* programmatically list supported dialects (`#462 <https://github.com/reata/sqllineage/issues/462>`_)
* add versioning of package to cli (`#457 <https://github.com/reata/sqllineage/issues/457>`_)
* Add Support of DROP VIEW statements  (`#456 <https://github.com/reata/sqllineage/issues/456>`_)
* support split SQL statements without semicolon in tsql (`#384 <https://github.com/reata/sqllineage/issues/384>`_)

Bugfix
-------------
* SqlFluff RuntimeError Triggers Server Error 500 in Frontend (`#467 <https://github.com/reata/sqllineage/issues/467>`_)
* ignore lineage for analyze statement (`#459 <https://github.com/reata/sqllineage/issues/459>`_)

v1.4.7
======
:Date: August 27, 2023

Enhancement
-------------
* Support subquery in VALUES clause (`#432 <https://github.com/reata/sqllineage/issues/432>`_)
* Dialect='tsql' should return warning when no semicolons are detected (`#422 <https://github.com/reata/sqllineage/issues/422>`_)
* Restricting folder and files user can access from frontend (`#405 <https://github.com/reata/sqllineage/issues/405>`_)
* throw exception when the statement missing the semicolon as splitter (`#159 <https://github.com/reata/sqllineage/issues/159>`_)

Bugfix
-------------
* AttributeError raised using parenthesized where clause (`#426 <https://github.com/reata/sqllineage/issues/426>`_)
* qualified wildcard recognized as wrong column name (`#423 <https://github.com/reata/sqllineage/issues/423>`_)

v1.4.6
======
:Date: July 31, 2023

In this release, we finally reach the milestone to make all sqlparse only test cases passed with sqlfluff implementation.
That's a big step in ultimately deprecating sqlparse. Also by upgrading to latest version of sqlfluff (with our PR merged),
we enjoy the benefits of improved sqlfluff performance when parsing some SQLs with nested query pattern.

Enhancement
-------------
* Improve sqlfluff Performance Issue on Nested Query Pattern (`#348 <https://github.com/reata/sqllineage/issues/348>`_)
* Reduce sqlparse only test cases (`#347 <https://github.com/reata/sqllineage/issues/347>`_)

Bugfix
-------------
* Missing Source Table for MERGE statement when UNION involved in source subquery (`#406 <https://github.com/reata/sqllineage/issues/406>`_)
* Column lineage does not work for CAST to Parameterized Data Type (`#329 <https://github.com/reata/sqllineage/issues/329>`_)
* Can't handle parenthesized from clause (`#278 <https://github.com/reata/sqllineage/issues/278>`_)

v1.4.5
======
:Date: July 2, 2023

Enhancement
-------------
* Switch to PyPI Trusted Publishers (`#389 <https://github.com/reata/sqllineage/issues/389>`_)
* Support tsql Declare Statement (`#357 <https://github.com/reata/sqllineage/issues/357>`_)

Bugfix
-------------
* Exception for Subquery Expression Without Source Tables (`#401 <https://github.com/reata/sqllineage/issues/401>`_)
* Not Supporting Create Table AS in postgres (`#400 <https://github.com/reata/sqllineage/issues/400>`_)
* Failed to handle UNION followed by CTE (`#398 <https://github.com/reata/sqllineage/issues/398>`_)
* Not handling CTE inside DML query (`#377 <https://github.com/reata/sqllineage/issues/377>`_)
* Failed to parse UNION inside CTE (`#376 <https://github.com/reata/sqllineage/issues/376>`_)

v1.4.4
======
:Date: June 11, 2023

Enhancement
-------------
* BigQuery Specific MERGE statement feature support (`#380 <https://github.com/reata/sqllineage/issues/380>`_)
* Support snowflake create table...clone and alter table...swap (`#373 <https://github.com/reata/sqllineage/issues/373>`_)
* Parse Column Lineage When Specify Column Names in Insert/Create Statement (`#212 <https://github.com/reata/sqllineage/issues/212>`_)

Bugfix
-------------
* Switching Dialect in UI only works When Explicit Clicked (`#387 <https://github.com/reata/sqllineage/issues/387>`_)
* No Column Lineage Parsed for DML with SELECT query in parenthesis (`#244 <https://github.com/reata/sqllineage/issues/244>`_)

v1.4.3
======
:Date: May 13, 2023

Enhancement
-------------
* Support postgres style type casts "keyword::TIMESTAMP" (`#364 <https://github.com/reata/sqllineage/issues/364>`_)

Bugfix
-------------
* Missing column lineage from SELECT DISTINCT using non-validating dialect (`#356 <https://github.com/reata/sqllineage/issues/356>`_)
* Missing column lineage with Parenthesis around column arithmetic operation (`#355 <https://github.com/reata/sqllineage/issues/355>`_)
* Not Handling CTE at the start of query in DML (`#328 <https://github.com/reata/sqllineage/issues/328>`_)

v1.4.2
======
:Date: April 22, 2023

Bugfix
-------------
* sqlparse v0.4.4 breaks non-validating dialect (`#361 <https://github.com/reata/sqllineage/issues/361>`_)

v1.4.1
======
:Date: April 2, 2023

Bugfix
-------------
* frontend app unable to load dialect when launched for the first time

v1.4.0
======
:Date: March 31, 2023

Great thanks to Nahuel, Mayur and Pere from OpenMetadata community for contributing on feature Dialect-awareness lineage.
Leveraging sqlfluff underneath, we're now able to give more correct lineage result with user input on SQL dialect.

Feature
-------------
* Dialect-awareness lineage (`#302 <https://github.com/reata/sqllineage/issues/302>`_)
* support MERGE statement (`#166 <https://github.com/reata/sqllineage/issues/166>`_)

Enhancement
-------------
* Use curved lines in lineage graph visualization (`#320 <https://github.com/reata/sqllineage/issues/320>`_)
* Click to lock highlighted nodes in visualization (`#318 <https://github.com/reata/sqllineage/issues/318>`_)
* Deprecate support for Python 3.6 and Python 3.7, add support for Python 3.11 (`#319 <https://github.com/reata/sqllineage/issues/319>`_)
* support t-sql assignment operator (`#205 <https://github.com/reata/sqllineage/issues/205>`_)

Bugfix
-------------
* exception when insert into qualified table followed by parenthesized query (`#249 <https://github.com/reata/sqllineage/issues/249>`_)
* missing columns when current_timestamp as reserved keyword used in select clause (`#248 <https://github.com/reata/sqllineage/issues/248>`_)
* exception when non-reserved keywords used as column name (`#183 <https://github.com/reata/sqllineage/issues/183>`_)
* exception when non-reserved keywords used as table name (`#93 <https://github.com/reata/sqllineage/issues/93>`_)

v1.3.7
======
:Date: Oct 22, 2022

Enhancement
-------------
* migrate demo site off Heroku to GitHub Pages (`#288 <https://github.com/reata/sqllineage/issues/288>`_)
* remove flask-related dependencies by implementing a wsgi app (`#287 <https://github.com/reata/sqllineage/issues/287>`_)

Bugfix
-------------
* exception with VALUES clause (`#292 <https://github.com/reata/sqllineage/issues/292>`_)
* exception with Presto unnest function (`#272 <https://github.com/reata/sqllineage/issues/272>`_)
* exception with snowflake generator statement (`#214 <https://github.com/reata/sqllineage/issues/214>`_)

v1.3.6
======
:Date: Aug 28, 2022

Enhancement
-------------
* support MySQL RENAME TABLE statement (`#267 <https://github.com/reata/sqllineage/issues/267>`_)
* auto deploy to Heroku with GitHub Actions (`#232 <https://github.com/reata/sqllineage/issues/232>`_)

Bugfix
-------------
* handling parenthesis around subquery between union (`#270 <https://github.com/reata/sqllineage/issues/270>`_)
* unable to extract alias of columns using function with CTAS (`#253 <https://github.com/reata/sqllineage/issues/253>`_)
* exception when using lateral view (`#225 <https://github.com/reata/sqllineage/issues/225>`_)

v1.3.5
======
:Date: May 10, 2022

Enhancement
-------------
* support parsing column in cast/try_cast with function (`#254 <https://github.com/reata/sqllineage/issues/254>`_)
* support parsing WITH for bucketing in Trino (`#251 <https://github.com/reata/sqllineage/issues/251>`_)

Bugfix
-------------
* incorrect column lineage with nested cast (`#240 <https://github.com/reata/sqllineage/issues/240>`_)
* column lineages from boolean expression (`#236 <https://github.com/reata/sqllineage/issues/236>`_)
* using JOIN with ON/USING keyword fails to determine source tables when followed by a parenthesis (`#233 <https://github.com/reata/sqllineage/issues/233>`_)
* failure to handle multiple lineage path for same column (`#228 <https://github.com/reata/sqllineage/issues/228>`_)

v1.3.4
======
:Date: March 6, 2022

Enhancement
-------------
* update black to stable version (`#222 <https://github.com/reata/sqllineage/issues/222>`_)

Bugfix
-------------
* table/column lineage mixed up for self dependent SQL (`#219 <https://github.com/reata/sqllineage/issues/219>`_)
* problem with SELECT CAST(CASE WHEN ...END AS DECIMAL(M,N)) AS col_name (`#215 <https://github.com/reata/sqllineage/issues/215>`_)
* failed to parse source table from subquery with more than one parenthesis (`#213 <https://github.com/reata/sqllineage/issues/213>`_)

v1.3.3
======
:Date: December 26, 2021

Enhancement
-------------
* smarter column-to-table resolution using query context (`#203 <https://github.com/reata/sqllineage/issues/203>`_)

Bugfix
-------------
* column lineage for union operation (`#207 <https://github.com/reata/sqllineage/issues/207>`_)
* subquery in where clause not parsed for table lineage (`#204 <https://github.com/reata/sqllineage/issues/204>`_)

v1.3.2
======
:Date: December 12, 2021

Enhancement
-------------
* support optional AS keyword in CTE (`#198 <https://github.com/reata/sqllineage/issues/198>`_)
* support referring to a CTE in subsequent CTEs (`#196 <https://github.com/reata/sqllineage/issues/196>`_)
* support for Redshift 'copy from' syntax (`#164 <https://github.com/reata/sqllineage/issues/164>`_)

v1.3.1
======
:Date: December 5, 2021

Enhancement
-------------
* test against Python 3.10 (`#186 <https://github.com/reata/sqllineage/issues/186>`_)

Bugfix
-------------
* alias parsed as table name for column lineage using ANSI-89 Join (`#190 <https://github.com/reata/sqllineage/issues/190>`_)
* CTE parsed as source table when referencing column from cte using alias (`#189 <https://github.com/reata/sqllineage/issues/189>`_)
* window function with parameter parsed as two columns (`#184 <https://github.com/reata/sqllineage/issues/184>`_)

v1.3.0
======
:Date: November 13, 2021

Feature
-------------
* Column-Level Lineage (`#103 <https://github.com/reata/sqllineage/issues/103>`_)

Bugfix
-------------
* SHOW CREATE TABLE parsed as target table (`#167 <https://github.com/reata/sqllineage/issues/167>`_)

v1.2.4
======
:Date: June 14, 2021

Enhancement
-------------
* highlight selected node and its ancestors as well as children recursively (`#156 <https://github.com/reata/sqllineage/issues/156>`_)
* add support for database.schema.table as identifier name (`#153 <https://github.com/reata/sqllineage/issues/153>`_)
* add support for swap_partitions_between_tables (`#152 <https://github.com/reata/sqllineage/issues/152>`_)

v1.2.3
======
:Date: May 15, 2021

Enhancement
-------------
* lineage API response exception handling (`#148 <https://github.com/reata/sqllineage/issues/148>`_)

v1.2.2
======
:Date: May 5, 2021

Bugfix
-------------
* resize dragger remain on the UI when drawer is closed (`#145 <https://github.com/reata/sqllineage/issues/145>`_)

v1.2.1
======
:Date: May 3, 2021

Enhancement
-------------
* option to specify hostname (`#142 <https://github.com/reata/sqllineage/issues/142>`_)
* re-sizable directory tree drawer (`#140 <https://github.com/reata/sqllineage/issues/140>`_)
* async loading for directory tree in frontend UI (`#138 <https://github.com/reata/sqllineage/issues/138>`_)

v1.2.0
======
:Date: April 18, 2021

Feature
-------------
* A Full Fledged Frontend Visualization App (`#118 <https://github.com/reata/sqllineage/issues/118>`_)
* Use TPC-DS Queries as Visualization Example (`#116 <https://github.com/reata/sqllineage/issues/116>`_)

Enhancement
-------------
* Unit Test Failure With sqlparse==0.3.0, update dependency to be >=0.3.1 (`#117 <https://github.com/reata/sqllineage/issues/117>`_)
* contributing guide (`#14 <https://github.com/reata/sqllineage/issues/14>`_)

v1.1.4
======
:Date: March 9, 2021

Bugfix
-------------
* trim function with from in arguments (`#127 <https://github.com/reata/sqllineage/issues/127>`_)

v1.1.3
======
:Date: February 1, 2021

Bugfix
-------------
* UNCACHE TABLE statement parsed with target table (`#123 <https://github.com/reata/sqllineage/issues/123>`_)

v1.1.2
======
:Date: January 26, 2021

Bugfix
-------------
* Bring back draw method of LineageRunner to avoid backward incompatible change (`#120 <https://github.com/reata/sqllineage/issues/120>`_)

v1.1.1
======
:Date: January 24, 2021

Bugfix
-------------
* SQLLineageException for Multiple CTE Subclauses (`#115 <https://github.com/reata/sqllineage/issues/115>`_)

v1.1.0
======
:Date: January 17, 2021

Feature
-------------
* A new JavaScript-based approach for visualization, drop dependency for graphviz (`#94 <https://github.com/reata/sqllineage/issues/94>`_)

Enhancement
-------------
* Test against Mac OS and Windows (`#87 <https://github.com/reata/sqllineage/issues/87>`_)

Bugfix
-------------
* buckets parsed as table name for Spark bucket table DDL (`#111 <https://github.com/reata/sqllineage/issues/111>`_)
* incorrect result for update statement (`#105 <https://github.com/reata/sqllineage/issues/105>`_)

v1.0.2
======
:Date: November 17, 2020

Enhancement
-------------
* black check in CI (`#99 <https://github.com/reata/sqllineage/issues/99>`_)
* switch to GitHub Actions for CI (`#95 <https://github.com/reata/sqllineage/issues/95>`_)
* test against Python 3.9 (`#84 <https://github.com/reata/sqllineage/issues/84>`_)

Bugfix
-------------
* cartesian product exception with ANSI-89 syntax (`#89 <https://github.com/reata/sqllineage/issues/89>`_)


v1.0.1
======
:Date: October 17, 2020

Enhancement
-------------
* remove upper bound for dependencies (`#85 <https://github.com/reata/sqllineage/issues/85>`_)

v1.0.0
======
:Date: September 27, 2020

New Features
-------------
* a detailed documentation hosted by readthedocs (`#81 <https://github.com/reata/sqllineage/issues/81>`_)

Enhancement
-------------
* drop support for Python 3.5 (`#79 <https://github.com/reata/sqllineage/issues/79>`_)

v0.4.0
======

:Date: August 29, 2020

New Features
-------------
* DAG based lineage representation with visualization functionality (`#55 <https://github.com/reata/sqllineage/issues/55>`_)

Enhancement
-------------
* replace print to stderr with logging (`#75 <https://github.com/reata/sqllineage/issues/75>`_)
* sort by table name in LineageResult (`#70 <https://github.com/reata/sqllineage/issues/70>`_)
* change schema default value from <unknown> to <default> (`#69 <https://github.com/reata/sqllineage/issues/69>`_)
* set up Github actions for PyPi publish (`#68 <https://github.com/reata/sqllineage/issues/68>`_)

v0.3.0
======

:Date: July 19, 2020

New Features
-------------
* statement granularity lineage result (`#32 <https://github.com/reata/sqllineage/issues/32>`_)
* schema aware parsing (`#20 <https://github.com/reata/sqllineage/issues/20>`_)

Enhancement
-------------
* allow user to specify combiner (`#64 <https://github.com/reata/sqllineage/issues/64>`_)
* trim leading comment for statement in verbose output (`#57 <https://github.com/reata/sqllineage/issues/57>`_)
* add mypy as static type checker (`#50 <https://github.com/reata/sqllineage/issues/50>`_)
* add bandit as security issue checker (`#48 <https://github.com/reata/sqllineage/issues/48>`_)
* enforce black as code formatter (`#46 <https://github.com/reata/sqllineage/issues/46>`_)
* dedicated Table/Partition/Column Class (`#31 <https://github.com/reata/sqllineage/issues/31>`_)
* friendly exception handling (`#30 <https://github.com/reata/sqllineage/issues/30>`_)

Bugfix
-------------
* subquery without alias raises exception (`#62 <https://github.com/reata/sqllineage/issues/62>`_)
* refresh table and cache table should not count as target table (`#59 <https://github.com/reata/sqllineage/issues/59>`_)
* let user choose whether to filter temp table or not (`#23 <https://github.com/reata/sqllineage/issues/23>`_)


v0.2.0
======

:Date: April 11, 2020

Enhancement
-------------
* test against Python 3.8 (`#39 <https://github.com/reata/sqllineage/issues/39>`_)

Bugfix
-------------
* comment in line raise AssertionError (`#37 <https://github.com/reata/sqllineage/issues/37>`_)
* white space in left join (`#36 <https://github.com/reata/sqllineage/issues/36>`_)
* temp table checking (`#35 <https://github.com/reata/sqllineage/issues/35>`_)
* enable case-sensitive parsing (`#34 <https://github.com/reata/sqllineage/issues/34>`_)
* support for create table like statement (`#29 <https://github.com/reata/sqllineage/issues/29>`_)
* special treatment for DDL (`#28 <https://github.com/reata/sqllineage/issues/28>`_)
* empty statement return (`#25 <https://github.com/reata/sqllineage/issues/25>`_)
* drop table parsed as target table (`#21 <https://github.com/reata/sqllineage/issues/21>`_)
* multi-line sql causes AssertionError (`#18 <https://github.com/reata/sqllineage/issues/18>`_)
* subquery mistake alias as table name (`#16 <https://github.com/reata/sqllineage/issues/16>`_)

v0.1.0
======

:Date: July 26, 2019

New Features
-------------
* stable command line interface (`#2 <https://github.com/reata/sqllineage/issues/2>`_)

Enhancement
-------------
* combine setup.py and requirements.txt (`#6 <https://github.com/reata/sqllineage/issues/6>`_)
* combine tox and Travis CI (`#5 <https://github.com/reata/sqllineage/issues/5>`_)
* table-wise lineage with sufficient test cases (`#4 <https://github.com/reata/sqllineage/issues/4>`_)
* a startup docs for sqllineage's usage (`#3 <https://github.com/reata/sqllineage/issues/3>`_)
* pypi badges in README (`#1 <https://github.com/reata/sqllineage/issues/1>`_)

v0.0.1
======

:Date: June 16, 2019

New Features
-------------
initial public release

