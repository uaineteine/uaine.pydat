# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
import sys
from pathlib import Path

auto_libs_loc = str(Path('..', '..', "uainepydat").resolve())
sys.path.insert(0,auto_libs_loc)

project = 'Uaine.Pydat'
copyright = '2025, Daniel Stamer-Squair'
author = 'Daniel Stamer-Squair'
release = '2025'
# The short X.Y version.
version_file_path = str(Path('..', "..", 'meta', "version.txt").resolve())
version = "unkown"
with open(version_file_path, 'r') as file:
    version = file.read().strip()

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = []

templates_path = ['_templates']
exclude_patterns = []

extensions = [
    'sphinx.ext.autodoc',
    "sphinx.ext.autosummary"
]

autosummary_generate = True  # Turn on sphinx.ext.autosummary

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']
