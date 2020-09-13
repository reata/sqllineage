*************
DOs and DONTs
*************

DOs
===
* SQLLineage will continue to support most commonly used SQL system. Make best effort to be compatible.
* SQLLineage will stay mainly as a command line tool, as well as a Python utils library.

DONTs
=====
* Column-level lineage will not be included since that would require metadata information, or we won't be able to trace
  situation like `select *`. However, there's no unified metadata service for all kinds of SQL systems.
* Likewise for Partition-level lineage. Until we find a way to not involve metadata service, we will not go for this.
