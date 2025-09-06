# Uaine.Pydat

A python package to assist in data and database handling.

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0) [![PyPI Downloads](https://static.pepy.tech/badge/uainepydat)](https://pepy.tech/projects/uainepydat) ![Version 1.6.2](https://img.shields.io/badge/version-1.6.2-brightgreen)

-Daniel Stamer-Squair 

# Purpose

This contains a few functions that make data handling a little simpler, including a dedicated psv function, hash functions, data table cleaning methods and snippet queries for duckdb.

#### STABILITY STATUS 

[![Lint Check](https://github.com/uaineteine/duck_db_template/actions/workflows/lint_check.yaml/badge.svg)](https://github.com/uaineteine/duck_db_template/actions/workflows/lint_check.yaml) [![Packaging Test](https://github.com/uaineteine/uaine.pydat/actions/workflows/packaging_test.yml/badge.svg)](https://github.com/uaineteine/uaine.pydat/actions/workflows/packaging_test.yml) [![Import test](https://github.com/uaineteine/uaine.pydat/actions/workflows/import_package_test.yml/badge.svg)](https://github.com/uaineteine/uaine.pydat/actions/workflows/import_package_test.yml)

DEVELOPMENT:

[![Lint Check - dev](https://github.com/uaineteine/duck_db_template/actions/workflows/lint_check_dev.yaml/badge.svg)](https://github.com/uaineteine/duck_db_template/actions/workflows/lint_check_dev.yaml) [![Packaging Test - dev](https://github.com/uaineteine/uaine.pydat/actions/workflows/packaging_test_dev.yml/badge.svg)](https://github.com/uaineteine/uaine.pydat/actions/workflows/packaging_test_dev.yml) [![Import test - dev](https://github.com/uaineteine/uaine.pydat/actions/workflows/import_package_test_dev.yml/badge.svg)](https://github.com/uaineteine/uaine.pydat/actions/workflows/import_package_test_dev.yml)

# Description

A Python package that streamlines data handling, processing, and database operations through a collection of utility functions and tools.

Uaine.Pydat provides a comprehensive toolkit for data scientists, analysts, and developers working with structured data. The package simplifies common data manipulation tasks while offering specialized functionality for file operations, data transformation, table cleaning, and database interactions.

Key Features include File I/O Operations for reading/writing various file formats, listing files by extension, and managing system paths; Data Transformation tools for reshaping, converting, and manipulating data structures with minimal code; Data Cleaning methods to sanitize, standardize, and prepare data tables for analysis; DuckDB Integration with helper functions and snippet queries; Cryptographic Hashing for data integrity and anonymization; System Information utilities to gather system metrics and resource usage; Data Generation for testing and development scenarios; and Configuration Handling for XML, INI, and other formats.

Core Modules include dataio.py for data input/output operations and format conversion; fileio.py for file system operations and path management; datatransform.py for data structure transformation and manipulation; dataclean.py for data cleaning and standardization; duckfunc.py for DuckDB database interactions; datahash.py for cryptographic hashing functions; systeminfo.py for system information gathering; datagen.py for random data generation; and bitgen.py for low-level bit generation utilities.

Common Use Cases for this package include simplifying ETL workflows, streamlining data preparation for analysis and machine learning, managing database operations with less boilerplate code, generating test data for development, monitoring system resources during data processing tasks, and securing sensitive data through hashing and anonymization.

This package aims to reduce the complexity of common data handling tasks by providing ready-made solutions that follow best practices while remaining flexible enough to adapt to various data processing requirements. 

# Getting Started

[Follow the documentation here on github: https://uaineteine.github.io/uaine-pydat-docs/](https://uaineteine.github.io/uaine-pydat-docs/)

## Requirements

1. **Python**
2. **requests**: This library is used to send HTTP requests.
3. **pyreadstat**: This library is used for most read and writing of dataframes.
4. **pandas**: This library is used to store and manipulate memory held data.
5. **duckdb**: This library is used to create duck db requests and queries from its metadata tables
6. **wheel**: This is for making the package yourself
7. **twine**: This is for making the package yourself
8. **psutil**: This is for checking process availability of the machine
9. **lxml**: This for parsing xml files and content
10. **polars**: For lazy stream reading and writing of parquets
11. **azure-storage-blob**: For listing and downloading from blob containers
12. **tqdm**: For progress bars

# Changelog

See the full version history in the [changelog](meta/changelog.txt).
