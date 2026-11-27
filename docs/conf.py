# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# http://www.sphinx-doc.org/en/master/config

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))


# -- Project information -----------------------------------------------------
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.abspath(".."))

from sqllineage import NAME, VERSION  # noqa

project = NAME
copyright = f"2019-{datetime.now().year}, Reata"  # noqa
author = "Reata"

# The full version, including alpha/beta/rc tags
release = version = VERSION

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = ["sphinx.ext.autodoc", "sphinx.ext.intersphinx", "sphinx.ext.viewcode"]

# Resolve references to Python standard library objects (e.g.
# collections.abc.Callable) against the official Python documentation.
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}

# Both the class’ and the __init__ method’s docstring are concatenated and inserted.
autoclass_content = "both"

autodoc_default_options = {"member-order": "bysource"}

# Keep default values as written in the source. Otherwise autodoc renders
# DummyMetaDataProvider() as "<... object>", which is not valid Python; Sphinx
# then falls back to splitting the signature at every comma and nitpicky
# reports a bogus "dict[str" reference.
# https://github.com/sphinx-doc/sphinx/issues/14159
autodoc_preserve_defaults = True

# Turn unresolved cross-references into warnings. Read the Docs builds with
# fail_on_warning: true, so a stale module path in a docstring fails the build.
nitpicky = True

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx_rtd_theme"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ["_static"]
