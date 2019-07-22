# -*- coding: utf-8 -*-
#
# Configuration file for the Sphinx documentation builder.
#
# This file does only contain a selection of the most common options. For a
# full list see the documentation:
# http://www.sphinx-doc.org/en/master/config


# -- Project information ------------------------------------------------------

# General information about the project.
copyright = '2018, Oslo Contributors'

# -- General configuration ----------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.todo',
    'openstackdocstheme',
    'stevedore.sphinxext',
    'oslo_config.sphinxext',
]

# openstackdocstheme options
repository_name = 'openstack/oslo.messaging'
bug_project = 'oslo.messaging'
bug_tag = ''

# The master toctree document.
master_doc = 'index'

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'sphinx'

# -- Options for HTML output --------------------------------------------------

# The theme to use for HTML and HTML Help pages.  Major themes that come with
# Sphinx are currently 'default' and 'sphinxdoc'.
html_theme = 'openstackdocs'
