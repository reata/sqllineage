SQLLineage: SQL Lineage Analysis Tool Powered by Python
=======================================================

Never get the hang of a SQL parser? SQLLineage comes to the rescue. Given a SQL command, SQLLineage will tell you its
source and target tables, without worrying about Tokens, Keyword, Identified and all the jagons used by a SQL parser.

Behind the scene, SQLLineage uses the fantastic `sqlparse`_ library to parse the SQL command, and bring you all the
human-readable result with ease.

.. _sqlparse: https://github.com/andialbrecht/sqlparse

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


Behind the scene
================

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Behind the scene

   behind_the_scene/why_sqllineage
   behind_the_scene/how_sqllineage_work
   behind_the_scene/dos_and_donts

:doc:`behind_the_scene/why_sqllineage`
    The motivation of writing SQLLineage

:doc:`behind_the_scene/how_sqllineage_work`
    The inner mechanism of SQLLineage

:doc:`behind_the_scene/dos_and_donts`
    Design principles for SQLLineage


Basic concepts
==============

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Basic concepts

   basic_concepts/lineage_runner
   basic_concepts/lineage_analyzer
   basic_concepts/lineage_combiner
   basic_concepts/lineage_model

:doc:`basic_concepts/lineage_runner`
    LineageRunner: The entry point for SQLLineage

:doc:`basic_concepts/lineage_analyzer`
    LineageAnalyzer: The core functionality of analyze one SQL statement

:doc:`basic_concepts/lineage_combiner`
    LineageCombiner: To combine all the atomic lineage result together into a Directed Acyclic Graph

:doc:`basic_concepts/lineage_model`
    The data classes for SQLLineage


Release note
============

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Release note

   release_note/changelog

:doc:`release_note/changelog`
    See what's new for each SQLLineage version
