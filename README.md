# STATUS v1.0

[![Execution tests](https://github.com/uaineteine/ArxivAPI/actions/workflows/execution-tests.yml/badge.svg)](https://github.com/uaineteine/ArxivAPI/actions/workflows/execution-tests.yml)

[![Packaging Test](https://github.com/uaineteine/ArxivAPI/actions/workflows/packaging_test.yml/badge.svg)](https://github.com/uaineteine/ArxivAPI/actions/workflows/packaging_test.yml)

# Uaine PyDat

A python package to assist in data and database handling.

# Purpose

This is a Python script that primarily uses the ArXiv API to scrape information about research papers. As a standard, the script sends a request to the ArXiv API with specific search parameters. The API returns a list of papers that match these parameters. The script then parses this data and extracts the relevant information about each paper, such as the title, authors, abstract, and ArXiv identifier.

In addition to the ArXiv API, the script also incorporates BeautifulSoup for direct HTTP scraping as a backup. This means that if the ArXiv API is unavailable or fails to return the desired information, the script can fall back on BeautifulSoup to scrape the data directly from the ArXiv website. This ensures that the script remains functional and reliable even under less than ideal circumstances.

# Requirements

1. **Python**
2. **requests**: This library is used to send HTTP requests to the ArXiv API.
4. **pandas**: This library is used to store and manipulate the scraped data.
5. **duckdb**: This library is used to create duck db requests and queries from its metadata tables

#### Daniel Stamer-Squair
