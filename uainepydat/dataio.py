import os
import configparser
import pandas as pd
import json
from io import StringIO
from uainepydat import fileio
from uainepydat import datatransform

def read_sas_metadata(filepath: str, encoding: str = "latin-1") -> dict:
    """
    Read SAS file metadata and return names, labels, formats, and lengths of columns.

    Args:
        filepath (str): The path to the SAS file.
        encoding (str): The encoding to use for reading the SAS file. Default is "latin-1".

    Returns:
        dict: A dictionary containing the column names, labels, formats, and lengths.
    """
    df = pd.read_sas(filepath, chunksize=1, encoding=encoding)
    sascols = df.columns(df)
    return {
        "names":  [a.name for a in sascols],
        "labels": [a.label for a in sascols],
        "format": [a.format for a in sascols],
        "length": [a.length for a in sascols]
    }

def read_sas_colnames(filepath: str, encoding: str = "latin-1") -> list:
    """
    Read SAS file column names.

    Args:
        filepath (str): The path to the SAS file.
        encoding (str): The encoding to use for reading the SAS file. Default is "latin-1".

    Returns:
        list: A list of column names from the SAS file.
    """
    metadata = read_sas_metadata(filepath, encoding=encoding)
    return metadata["names"]

def write_flat_df(df: pd.DataFrame, filepath: str, index: bool = False):
    """
    Write a DataFrame to a flat file in different formats.

    Args:
        df (pd.DataFrame): The DataFrame to be written.
        filepath (str): The path where the file will be saved.
        index (bool): Whether to write row names (index). Default is False.

    Returns:
        None
    """
    format = fileio.get_file_extension(filepath)
    if format == "csv":
        df.to_csv(filepath, chunksize=50000, index=index)
    elif format in ("xlsx", "xls"):
        df.to_excel(filepath, index=index)
    elif format == "parquet":
        df.to_parquet(filepath, index=index)
    elif format == "psv":
        df.to_csv(filepath, sep="|", index=index)
    elif format == "json":
        write_json_file(df, filepath, index=index)
    elif format == "xml":
        write_xml_file(df, filepath, index=index)
    else:
        raise ValueError(f"Unsupported file extension {format}")

def read_flat_df(filepath: str) -> pd.DataFrame:
    """
    Read a flat file into a DataFrame.

    Args:
        filepath (str): The path to the flat file.

    Returns:
        pd.DataFrame: The DataFrame read from the file.
    """
    if (os.path.exists(filepath) == False):
        raise FileNotFoundError(f"File {filepath} does not exist")
    
    format = fileio.get_file_extension(filepath)
    if format == "csv":
        return pd.read_csv(filepath)
    elif format in ("xlsx", "xls"):
        return pd.read_excel(filepath)
    elif format == "parquet":
        return pd.read_parquet(filepath)
    elif format == "psv":
        return read_flat_psv(filepath)
    elif format == "sas7bdat":
        return pd.read_sas(filepath)
    elif format == "json":
        return read_json_file(filepath)
    elif format == "xml":
        return read_xml_file(filepath)
    else:
        raise ValueError(f"Unsupported file extension {format}")
    return None

def read_flat_psv(path: str) -> pd.DataFrame:
    """
    Read a pipe-separated values (PSV) file into a DataFrame.

    Args:
        path (str): The path to the PSV file.

    Returns:
        pd.DataFrame: The DataFrame read from the PSV file.
    """
    if (os.path.exists(path) == False):
        raise FileNotFoundError(f"File {path} does not exist")
    return pd.read_csv(path, delimiter='|')

#read the config file
def read_ini_file(file_path: str) -> dict:
    """
    Read an INI file and return its contents as a dictionary.

    Args:
        file_path (str): The path to the INI file.

    Returns:
        dict: A dictionary containing the key-value pairs from the INI file.
    """
    config = configparser.ConfigParser()
    if (os.path.exists(file_path) == False):
        raise FileNotFoundError(f"File {file_path} does not exist")
    config.read(file_path)

    variables = {}
    for section in config.sections():
        for key, value in config.items(section):
            variables[key] = value

    return variables

import configparser

# Define a function to set variables as global
def set_globals_from_config(configpath: str) -> int:
    """
    Sets global variables from a configuration file.

    Parameters:
    configpath (str): Path to the configuration file.

    Returns:
    int: The number of global variables set.
    """
    # Read the configuration file
    configvars = read_ini_file(configpath)

    # Loop through the configuration variables and set them as globals
    for key, var in configvars.items():
        globals()[key] = var

    # Return the number of global variables set
    return len(configvars)

def select_dataset_ui(directory: str, extension: str) -> str:
    """
    List the files with the specified extension in the given directory and prompt the user to select one.

    Parameters:
    directory (str): The directory to search for files.
    extension (str): The file extension to filter by.

    Returns:
    str: The filename of the selected dataset.
    """
    files = fileio.list_files_of_extension(directory, extension)
    for i, filename in enumerate(files):
        print(f"{i+1}. {filename}")
    selected = int(input("Enter the number of the dataset you want to select: ")) - 1
    return files[selected]

def df_memory_usage(df: pd.DataFrame) -> float:
    """
    Calculate the total memory usage of a DataFrame with deep=False.
    
    Parameters:
    df (pd.DataFrame): The DataFrame whose memory usage is to be calculated.
    
    Returns:
    float: The total memory usage of the DataFrame in bytes.
    """
    # Calculate memory usage of the DataFrame with deep=False
    memory_usage = df.memory_usage(deep=False)
    # Sum up the memory usage of all columns
    total_memory_usage = memory_usage.sum()
    return total_memory_usage

def read_json_file(filepath: str, orient: str = 'records', normalize: bool = False, 
                  record_path: str = None, meta: list = None, encoding: str = 'utf-8') -> pd.DataFrame:
    """
    Read a JSON file into a DataFrame.

    Args:
        filepath (str): The path to the JSON file.
        orient (str): The format of the JSON structure. Default is 'records'.
        normalize (bool): Whether to normalize nested JSON data. Default is False.
        record_path (str or list): Path to the records in nested JSON. Default is None.
        meta (list): Fields to use as metadata for each record. Default is None.
        encoding (str): The file encoding. Default is 'utf-8'.

    Returns:
        pd.DataFrame: The DataFrame read from the JSON file.
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File {filepath} does not exist")
    
    # Use the existing json_to_dataframe function from datatransform module
    return datatransform.json_to_dataframe(
        filepath, orient=orient, normalize=normalize, 
        record_path=record_path, meta=meta, encoding=encoding
    )

def write_json_file(df: pd.DataFrame, filepath: str, orient: str = 'records', 
                   index: bool = False, indent: int = 4) -> None:
    """
    Write a DataFrame to a JSON file.

    Args:
        df (pd.DataFrame): The DataFrame to be written.
        filepath (str): The path where the JSON file will be saved.
        orient (str): The format of the JSON structure. Default is 'records'.
        index (bool): Whether to include the index in the JSON. Default is False.
        indent (int): The indentation level for the JSON file. Default is 4.
    
    Returns:
        None
    """
    df.to_json(filepath, orient=orient, indent=indent, index=index)
    
def read_xml_file(filepath: str, xpath: str = './*', attrs_only: bool = False, 
                 encoding: str = 'utf-8') -> pd.DataFrame:
    """
    Read an XML file into a DataFrame.

    Args:
        filepath (str): The path to the XML file.
        xpath (str): XPath string to parse specific nodes. Default is './*'.
        attrs_only (bool): Parse only the attributes, not the child elements. Default is False.
        encoding (str): The file encoding. Default is 'utf-8'.

    Returns:
        pd.DataFrame: The DataFrame read from the XML file.
    """
    try:
        return pd.read_xml(filepath, xpath=xpath, attrs_only=attrs_only, encoding=encoding)
    except ImportError:
        raise ImportError("pandas version with XML support required. Please update pandas.")
    except Exception as e:
        raise ValueError(f"Error reading XML file: {e}")

def write_xml_file(df: pd.DataFrame, filepath: str, index: bool = False, 
                  root_name: str = 'data', row_name: str = 'row',
                  attr_cols: list = None) -> None:
    """
    Write a DataFrame to an XML file.

    Args:
        df (pd.DataFrame): The DataFrame to be written.
        filepath (str): The path where the XML file will be saved.
        index (bool): Whether to include the index in the XML. Default is False.
        root_name (str): The name of the root element. Default is 'data'.
        row_name (str): The name of each row element. Default is 'row'.
        attr_cols (list): List of columns to write as attributes, not elements. Default is None.

    Returns:
        None
    """
    try:
        df.to_xml(filepath, index=index, root_name=root_name, row_name=row_name, attr_cols=attr_cols)
    except ImportError:
        raise ImportError("pandas version with XML support required. Please update pandas.")
    except Exception as e:
        raise ValueError(f"Error writing XML file: {e}")
