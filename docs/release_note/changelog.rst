*********
Changelog
*********

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

