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
* Column-level lineage will not be 100% accurate because that would require metadata information. However, there's no
  unified metadata service for all kinds of SQL systems. For the moment, in column-level lineage, column-to-table
  resolution is conducted in a best-effort way, meaning we only provide possible table candidates for situation like
  ``select *`` or ``select col from tab1 join tab2``.
* Likewise for Partition-level lineage. Until we find a way to not involve metadata service, we will not go for this.

.. note::
    100% accurate Column-level lineage is still do-able if we can provide some kind of a plugin system for user to
    register their metadata instead of us maintaining it. Let's see what will happen in future versions.
