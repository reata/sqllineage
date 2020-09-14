*************
DOs and DONTs
*************

SQLLineage is a static SQL code lineage analysis tool, which means we will not try to execute the SQL code against any
kinds of server. Instead, we will just look at the code as text, parse it and give back the result. No client/server
interaction.

DOs
===
* SQLLineage will continue to support most commonly used SQL system. Make best effort to be compatible.
* SQLLineage will stay mainly as a command line tool, as well as a Python utils library.

DONTs
=====
* Column-level lineage will not be included since that would require metadata information, or we won't be able to trace
  situation like `select *`. However, there's no unified metadata service for all kinds of SQL systems.
* Likewise for Partition-level lineage. Until we find a way to not involve metadata service, we will not go for this.
