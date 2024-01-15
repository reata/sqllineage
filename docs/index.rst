SQLLineage: SQL Lineage Analysis Tool Powered by Python
=======================================================

Never get the hang of a SQL parser? SQLLineage comes to the rescue. Given a SQL command, SQLLineage will tell you its
source and target tables, without worrying about Tokens, Keyword, Identified and all the jagons used by a SQL parser.

Behind the scene, SQLLineage pluggable leverages parser library `sqlfluff`_ and `sqlparse`_ to parse the SQL command,
analyze the AST, stores the lineage information in a graph (using graph library `networkx`_), and bring you all the
human-readable result with ease.

.. _sqlfluff: https://github.com/sqlfluff/sqlfluff
.. _sqlparse: https://github.com/andialbrecht/sqlparse
.. _networkx: https://github.com/networkx/networkx

First steps
===========

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: First steps

   first_steps/getting_started
   first_steps/advanced_usage
   first_steps/beyond_command_line

:doc:`first_steps/getting_started`
    Install SQLLineage and quick use the handy built-in command-line tool

:doc:`first_steps/advanced_usage`
    Some advanced usage like multi statement SQL lineage and lineage visualization

:doc:`first_steps/beyond_command_line`
    Using SQLLineage in your Python script


Gear Up
=======

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Gear up

   gear_up/configuration
   gear_up/metadata

:doc:`gear_up/configuration`
    Learn how to configure sqllineage

:doc:`gear_up/metadata`
    Learn how to use MetaDataProvider


Behind the scene
================

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Behind the scene

   behind_the_scene/why_sqllineage
   behind_the_scene/how_sqllineage_work
   behind_the_scene/dos_and_donts
   behind_the_scene/column-level_lineage_design
   behind_the_scene/dialect-awareness_lineage_design

:doc:`behind_the_scene/why_sqllineage`
    The motivation of writing SQLLineage

:doc:`behind_the_scene/how_sqllineage_work`
    The inner mechanism of SQLLineage

:doc:`behind_the_scene/dos_and_donts`
    Design principles for SQLLineage

:doc:`behind_the_scene/column-level_lineage_design`
    Design docs for column lineage

:doc:`behind_the_scene/dialect-awareness_lineage_design`
    Design docs for dialect-awareness lineage

Basic concepts
==============

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Basic concepts

   basic_concepts/runner
   basic_concepts/analyzer
   basic_concepts/holder
   basic_concepts/model
   basic_concepts/metadata_provider

:doc:`basic_concepts/runner`
    LineageRunner: The entry point for SQLLineage

:doc:`basic_concepts/analyzer`
    LineageAnalyzer: The core functionality of analyze one SQL statement

:doc:`basic_concepts/holder`
    LineageHolder: To hold lineage result at different level

:doc:`basic_concepts/model`
    The data classes for SQLLineage

:doc:`basic_concepts/metadata_provider`
    MetaDataProvider: provider metadata to assist lineage analysis

Release note
============

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Release note

   release_note/changelog

:doc:`release_note/changelog`
    See what's new for each SQLLineage version
