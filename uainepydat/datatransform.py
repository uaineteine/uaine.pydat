import pandas as pd
import json
import os
from io import StringIO
import re

def replace_between_tags(content: str, tag_name: str, new_lines: list[str], deleteTags=False) -> str:
    start_tag = f'<{tag_name}>'
    end_tag = f'</{tag_name}>'
    
    start_index = content.find(start_tag)
    end_index = content.find(end_tag, start_index)

    if start_index == -1 or end_index == -1:
        raise ValueError("Tags not found in the content")

    #delete tags themeselves by modifying the selection index
    if deleteTags:
        new_content = content[:start_index] + '\n'.join(new_lines) + content[end_index + len(end_tag):]
    else:
        new_content = content[:start_index + len(start_tag)] + '\n' + '\n'.join(new_lines) + '\n' + content[end_index:]

    return new_content

def break_into_lines(string: str) -> list[str]:
    """
    Breaks a string into a list of lines.

    Args:
        string (str): The input string to be broken into lines.

    Returns:
        list[str]: A list of lines from the input string.
    """
    return string.split('\n')


def add_prefix(string: str, prefix: str) -> str:
    """
    Add the specified prefix to the string.

    Parameters:
    string (str): The original string.
    prefix (str): The prefix to add to the string.

    Returns:
    str: The string with the prefix added.
    """
    return prefix + string

def add_suffix(string: str, suffix: str) -> str:
    """
    Add the specified suffix to the string.

    Parameters:
    string (str): The original string.
    suffix (str): The suffix to add to the string.

    Returns:
    str: The string with the suffix added.
    """
    return string + suffix

def json_to_dataframe(json_data, orient='records', normalize=False, record_path=None, meta=None, encoding='utf-8'):
    """
    Convert JSON data into a pandas DataFrame.

    Parameters:
    -----------
    json_data : str, dict, list, or path to file
        The JSON data to convert. Can be:
        - A string containing JSON data
        - A Python dict or list containing JSON data
        - A file path to a JSON file
    orient : str, default 'records'
        The JSON string orientation. Allowed values:
        - 'records': list-like [{column -> value}, ... ]
        - 'split': dict-like {'index' -> [index], 'columns' -> [columns], 'data' -> [values]}
        - 'index': dict-like {index -> {column -> value}}
        - 'columns': dict-like {column -> {index -> value}}
        - 'values': just the values array
    normalize : bool, default False
        Whether to normalize semi-structured JSON data into a flat table
    record_path : str or list of str, default None
        Path in each object to list of records. If not passed, data will be
        assumed to be an array of records.
    meta : list of str, default None
        Fields to use as metadata for each record in resulting DataFrame
    encoding : str, default 'utf-8'
        Encoding to use when reading JSON from a file

    Returns:
    --------
    pd.DataFrame
        The converted DataFrame

    Examples:
    ---------
    # From a JSON string
    >>> json_str = '{"name": "John", "age": 30, "city": "New York"}'
    >>> df = json_to_dataframe(json_str)

    # From a file
    >>> df = json_to_dataframe('data.json')

    # With nested data
    >>> json_str = '{"users": [{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]}'
    >>> df = json_to_dataframe(json_str, record_path='users')
    """

    # Check if input is a file path
    if isinstance(json_data, str) and os.path.isfile(json_data):
        with open(json_data, 'r', encoding=encoding) as f:
            json_data = json.load(f)
    
    # Check if input is a JSON string
    elif isinstance(json_data, str):
        try:
            json_data = json.loads(json_data)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON string: {e}")
    
    # Process nested JSON if normalize=True and record_path is provided
    if normalize and record_path is not None:
        return pd.json_normalize(json_data, record_path=record_path, meta=meta)
    
    # Otherwise use pandas' regular read_json functionality
    if isinstance(json_data, (dict, list)):
        # Convert to JSON string for pd.read_json
        json_str = json.dumps(json_data)
        
        # Handle single objects by converting them to a list
        if isinstance(json_data, dict):
            # For single objects, convert to a list with one item
            return pd.DataFrame([json_data])
        else:
            # For arrays/lists, use StringIO as recommended
            return pd.read_json(StringIO(json_str), orient=orient)
    else:
        raise ValueError("Input must be a valid JSON string, Python dict/list, or file path")

def dataframe_to_json(df: pd.DataFrame, orient: str = 'records', date_format: str = 'iso', indent: int = None) -> str:
    """
    Convert a DataFrame to a JSON string with various orientation options.

    Parameters:
    -----------
    df : pd.DataFrame
        The DataFrame to convert to JSON
    orient : str, default 'records'
        The JSON string orientation. See json_to_dataframe for options.
    date_format : str, default 'iso'
        Format for dates in the resulting JSON:
        - 'epoch': Use Unix epoch (seconds since 1970-01-01)
        - 'iso': ISO 8601 formatted dates
    indent : int, default None
        Indentation level for the resulting JSON string. None = no indentation.

    Returns:
    --------
    str
        JSON string representation of the DataFrame
    """
    return df.to_json(orient=orient, date_format=date_format, indent=indent)

def merge_json_objects(json1: dict, json2: dict, merge_lists: bool = False) -> dict:
    """
    Merge two JSON objects, with the second one taking precedence for overlapping keys.

    Parameters:
    -----------
    json1 : dict
        First JSON object (base)
    json2 : dict
        Second JSON object (takes precedence when keys overlap)
    merge_lists : bool, default False
        If True, merge list items; if False, replace lists entirely

    Returns:
    --------
    dict
        Merged JSON object
    """
    result = json1.copy()
    
    for key, value in json2.items():
        # If both values are dictionaries, merge them recursively
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = merge_json_objects(result[key], value, merge_lists)
        # If both values are lists and merge_lists is True, combine them
        elif key in result and isinstance(result[key], list) and isinstance(value, list) and merge_lists:
            result[key] = result[key] + value
        # Otherwise, value from json2 takes precedence
        else:
            result[key] = value
            
    return result

def json_extract_subtree(json_data, path: str) -> any:
    """
    Extract a subtree from a JSON object using a dot-notation path.

    Parameters:
    -----------
    json_data : dict or list
        The JSON data to extract from
    path : str
        Path to the subtree using dot notation (e.g., 'person.address.city')
        Use array indices like 'results.0.name' to access list elements

    Returns:
    --------
    any
        The subtree at the specified path, or None if path doesn't exist
        
    Examples:
    ---------
    >>> data = {'person': {'name': 'John', 'addresses': [{'city': 'New York'}, {'city': 'Boston'}]}}
    >>> json_extract_subtree(data, 'person.addresses.0.city')
    'New York'
    """
    parts = path.split('.')
    current = json_data
    
    for part in parts:
        # Handle array indices
        if part.isdigit() and isinstance(current, list):
            idx = int(part)
            if 0 <= idx < len(current):
                current = current[idx]
            else:
                return None
        # Handle dictionary keys
        elif isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return None
            
    return current

def xml_to_dataframe(xml_data, xpath: str = './*') -> pd.DataFrame:
    """
    Convert XML data to a pandas DataFrame.

    Parameters:
    -----------
    xml_data : str or file-like object or path
        The XML data to convert. Can be:
        - A string containing XML data
        - A file path to an XML file
        - A file-like object containing XML data
    xpath : str, default ./*
        XPath string to parse specific nodes

    Returns:
    --------
    pd.DataFrame
        The DataFrame representation of the XML data
    """
    try:
        if isinstance(xml_data, str):
            # Check if it's a file path
            if os.path.isfile(xml_data):
                return pd.read_xml(xml_data, xpath=xpath)
            # Otherwise, treat it as XML string
            return pd.read_xml(StringIO(xml_data), xpath=xpath)
        # Handle file-like objects
        return pd.read_xml(xml_data, xpath=xpath)
    except Exception as e:
        raise ValueError(f"Failed to convert XML to DataFrame: {e}")

def dataframe_to_xml(df: pd.DataFrame, root_name: str = 'data', row_name: str = 'row') -> str:
    """
    Convert a DataFrame to an XML string.

    Parameters:
    -----------
    df : pd.DataFrame
        The DataFrame to convert to XML
    root_name : str, default 'data'
        The name of the root XML element
    row_name : str, default 'row'
        The name of each row element

    Returns:
    --------
    str
        XML string representation of the DataFrame
    """
    return df.to_xml(root_name=root_name, row_name=row_name)

# Execution tests
#if __name__ == "__main__":
    # print("\n=== Testing replace_between_tags ===")
    # content = "Some text <tag>old content</tag> more text"
    # new_lines = ["new line 1", "new line 2"]
    # try:
    #     result = replace_between_tags(content, "tag", new_lines)
    #     print("With tags:", result)
    #     result = replace_between_tags(content, "tag", new_lines, deleteTags=True)
    #     print("Without tags:", result)
    # except ValueError as e:
    #     print(f"Error: {e}")

    # print("\n=== Testing break_into_lines ===")
    # test_str = "line1\nline2\nline3"
    # print("Input:", test_str)
    # print("Output:", break_into_lines(test_str))

    # print("\n=== Testing add_prefix and add_suffix ===")
    # test_str = "Hello"
    # print("Original:", test_str)
    # print("With prefix 'Greeting: ':", add_prefix(test_str, "Greeting: "))
    # print("With suffix ' World':", add_suffix(test_str, " World"))

    # print("\n=== Testing json_to_dataframe ===")
    # # Test with simple JSON string
    # json_str = '{"name": "John", "age": 30, "city": "New York"}'
    # print("\nSimple JSON:")
    # print(json_to_dataframe(json_str))

    # # Test with nested JSON
    # nested_json = {
    #     "users": [
    #         {"name": "John", "age": 30, "address": {"city": "New York"}},
    #         {"name": "Jane", "age": 25, "address": {"city": "Boston"}}
    #     ]
    # }
    # print("\nNested JSON:")
    # print(json_to_dataframe(nested_json, normalize=True, record_path='users'))

    # print("\n=== Testing dataframe_to_json ===")
    # df = pd.DataFrame({
    #     'name': ['John', 'Jane'],
    #     'age': [30, 25],
    #     'city': ['New York', 'Boston']
    # })
    # print("DataFrame to JSON (records):")
    # print(dataframe_to_json(df, orient='records', indent=2))

    # print("\n=== Testing merge_json_objects ===")
    # json1 = {"name": "John", "age": 30, "hobbies": ["reading"]}
    # json2 = {"age": 31, "hobbies": ["gaming"]}
    # print("Merged (without list merging):")
    # print(merge_json_objects(json1, json2))
    # print("\nMerged (with list merging):")
    # print(merge_json_objects(json1, json2, merge_lists=True))

    # print("\n=== Testing json_extract_subtree ===")
    # test_data = {
    #     "person": {
    #         "name": "John",
    #         "addresses": [
    #             {"city": "New York"},
    #             {"city": "Boston"}
    #         ]
    #     }
    # }
    # print("Extract 'person.name':", json_extract_subtree(test_data, "person.name"))
    # print("Extract 'person.addresses.0.city':", json_extract_subtree(test_data, "person.addresses.0.city"))

    # print("\n=== Testing xml_to_dataframe and dataframe_to_xml ===")
    # xml_str = """
    # <data>
    #     <row>
    #         <name>John</name>
    #         <age>30</age>
    #         <city>New York</city>
    #     </row>
    #     <row>
    #         <name>Jane</name>
    #         <age>25</age>
    #         <city>Boston</city>
    #     </row>
    # </data>
    # """
    # print("\nXML to DataFrame:")
    # df_xml = xml_to_dataframe(xml_str)
    # print(df_xml)
    
    # print("\nDataFrame back to XML:")
    # xml_output = dataframe_to_xml(df_xml, root_name='data', row_name='row')
    # print(xml_output)