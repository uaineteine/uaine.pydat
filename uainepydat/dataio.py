import configparser
import pandas as pd
from uainepydat import fileio
import pandas as pd

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
    else:
        raise ValueError

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
