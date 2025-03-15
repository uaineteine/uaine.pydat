import pandas as pd
import json
import os
from io import StringIO

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

#Executions test
json_str = '{"name": "John", "age": 30, "city": "New York"}'
df = json_to_dataframe(json_str)
print(df)