import os
import configparser
import pandas as pd
import polars as pl
from io import StringIO
from uainepydat import fileio
from uainepydat import datatransform
from typing import Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed

def csv_to_parquet(input_file: str, separator: str = ",", output_file: str= None) -> None:
    """
    Converts a CSV file to a Parquet file using Polars in streaming mode.
    
    Parameters:
      - input_file (str): The path to the CSV file.
      - separator (str): The delimiter used in the CSV (default: comma).
      - output_file (Optional[str]): The path for the output Parquet file.
         If not provided, it defaults to the same prefix as input_file with a .parquet extension.
    """
    if output_file is None:
        # Automatically derive the output file name from the input file
        base, _ = os.path.splitext(input_file)
        output_file = f"{base}.parquet"

    # Read the CSV file in streaming mode and then write to Parquet
    df = pl.scan_csv(input_file, separator=separator)
    df.sink_parquet(output_file)

def sas_to_parquet_chunks_mt(
    sas_file: str,
    out_dir: str,
    rows_per_chunk: int = 100_000,
    format: str = "sas7bdat",
    max_workers: int = 4,
    max_inflight: int = 8,  # cap number of chunks held in memory
    parquet_engine: str = "pyarrow"  # or 'fastparquet'
):
    """
    Convert a large SAS file into multiple Parquet files by processing it in chunks
    using multithreading for parallel writes.

    The function reads the input SAS dataset in chunks, writes each chunk
    as a Parquet file, and ensures that the number of in-memory chunks is capped
    to avoid excessive memory usage.

    Parameters
    ----------
    sas_file : str
        Path to the input SAS file (e.g., ``.sas7bdat``).
    out_dir : str
        Directory where the resulting Parquet files will be written. Created if it
        does not exist.
    rows_per_chunk : int, optional
        Number of rows per chunk to read and write. Defaults to ``100_000``.
    format : str, optional
        File format for the SAS reader (typically ``"sas7bdat"`` or ``"xport"``).
        Defaults to ``"sas7bdat"``.
    max_workers : int, optional
        Maximum number of worker threads for concurrent Parquet writing. Defaults to ``4``.
    max_inflight : int, optional
        Maximum number of chunks allowed to be pending in memory before waiting for
        some to finish. Defaults to ``8``.
    parquet_engine : str, optional
        Parquet backend to use. Supported values are ``"pyarrow"`` and ``"fastparquet"``.
        Defaults to ``"pyarrow"``.

    Returns
    -------
    None
        This function writes output files to ``out_dir`` and prints progress to stdout.

    Notes
    -----
    - Output files are named sequentially as ``part_00000.parquet``, ``part_00001.parquet``,
      and so on.
    - Uses :class:`concurrent.futures.ThreadPoolExecutor` for concurrent writes.
    - Uses ``pandas.read_sas`` with chunking enabled.
    - Helps process very large SAS datasets without loading them entirely into memory.

    Examples
    --------
    >>> sas_to_parquet_chunks_mt(
    ...     sas_file="data/huge_dataset.sas7bdat",
    ...     out_dir="parquet_chunks",
    ...     rows_per_chunk=50_000,
    ...     max_workers=8,
    ...     parquet_engine="fastparquet"
    ... )
    """
    def _write_chunk(chunk, out_path):
        chunk.to_parquet(out_path, engine=parquet_engine, index=False)
        return out_path

    def _wait_some(futures):
        """Wait until at least one future completes, return (done, not_done)."""
        from concurrent.futures import wait, FIRST_COMPLETED
        done, not_done = wait(futures, return_when=FIRST_COMPLETED)
        return done, list(not_done)

    os.makedirs(out_dir, exist_ok=True)

    reader = pd.read_sas(sas_file, format=format, chunksize=rows_per_chunk, encoding="latin-1")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for i, chunk in enumerate(reader):
            out_path = os.path.join(out_dir, f"part_{i:05d}.parquet")
            futures.append(executor.submit(_write_chunk, chunk, out_path))

            # prevent too many chunks from piling up in memory
            if len(futures) >= max_inflight:
                done, futures = _wait_some(futures)
                for f in done:
                    print(f"→ Wrote {f.result()}")

        # flush remaining
        for f in as_completed(futures):
            print(f"→ Wrote {f.result()}")

    print(f"Finished splitting {sas_file} into Parquet chunks at {out_dir}")

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

# def read_sas_chunk(filepath: str, offset: int, chunksize: int, use_cols: list = None) -> pd.DataFrame:
#     """
#     Reads a chunk of rows from SAS7BDAT file. Helper function to fetch_sas_parallel.

#     Args:
#         filepath (str): The filepath to the SAS dataset (.sas7bdat) file that is being converted to a Polars DataFrame
#         offset (int): The starting row for the read-in chunk relative to the data being read in. This is used to avoid overlapping of chunks,
#         chunksize (int): The number of rows to process at one time, per chunk. 

#     Returns:
#         pl.DataFrame: A Polars DataFrame representing the read chunk.
#     """

#     #if usecols is given, do a column check
#     if use_cols is not None:
#         columns_in_frame = read_sas_colnames(filepath, encoding=encoding)
#         if not all(col in columns_in_frame for col in use_cols):
#             raise ValueError(f"Column {col} not found in SAS file")

#     pandas_df, _ = pyreadstat.read_sas7bdat(
#         filename_path=filepath,
#         row_offset=offset,
#         row_limit=chunksize,
#         usecols=use_cols
#     )

#     return pandas_df


# def main():
#     """
#     Simple tests for read_sas_chunk. Update the 'test_filepath' to point to a valid .sas7bdat file for testing.
#     """
#     import pandas as pd
#     test_filepath = "test_chunk_for_sas/dataset.sas7bdat"  # TODO: Update this path
#     offset = 0
#     chunksize = 10

#     # Test 1: All columns
#     use_cols = None
#     try:
#         df = read_sas_chunk(test_filepath, offset, chunksize, use_cols)
#         print("Test 1 (all columns):")
#         print(df)
#         assert isinstance(df, pd.DataFrame), "Output is not a pandas DataFrame"
#         assert len(df) <= chunksize, "Returned chunk is larger than requested chunksize"
#         print("Test 1 passed.")
#     except Exception as e:
#         print(f"Test 1 failed: {e}")

#     # Test 2: Only 'var2' column
#     use_cols = ['var2']
#     try:
#         df2 = read_sas_chunk(test_filepath, offset, chunksize, use_cols)
#         print("Test 2 (only 'var2' column):")
#         print(df2)
#         assert isinstance(df2, pd.DataFrame), "Output is not a pandas DataFrame"
#         assert len(df2) <= chunksize, "Returned chunk is larger than requested chunksize"
#         assert list(df2.columns) == ['var2'], "Returned columns do not match ['var2']"
#         print("Test 2 passed.")
#     except Exception as e:
#         print(f"Test 2 failed: {e}")

# if __name__ == "__main__":
#     main()

# def validate_processes_count(num_processes: int) -> None:

#     CPU_USAGE_WARNING_THRESHOLD = 0.75
#     max_procs = mp.cpu_count()
#     proc_thresh = int(CPU_USAGE_WARNING_THRESHOLD * max_procs)
#     if num_processes > max_procs:
#         raise ValueError(
#             f"The specified number of processes ({num_processes}) exceeds the number of "
#             f"available CPU cores ({max_procs}). Use a value between 2 and {max_procs}."
#         )
#         if num_processes > proc_thresh:
#             print(
#                     f"Warning: The number of processes specified ({num_processes}) is greater than "
#                     f"{int(CPU_USAGE_WARNING_THRESHOLD * 100)}% of available CPU cores ({max_procs}).\n" 
#                     "This may impact system responsiveness or performance."
#             )
#             response = input("Do you want to continue? [y/n]: ").strip().lower()
#             if response not in ("y", "yes"):
#                 print("Aborting. Please specify a lower number of processes.")
#                 sys.exit(1)

# def sas_to_polars(
#     filepath: str,
#     chunksize: Optional[int] = 10_000,
#     processes: Optional[int] = mp.cpu_count() // 4,
#     use_lazy: Optional[bool] = True,
#     unordered: Optional[bool] = False
# ) -> pl.DataFrame:
#     """
#     Reads in a .sas7bdat file in parallel using multiple processes and returns a concatenated Polars DataFrame.

#     Args:
#         filepath (str): The filepath to the SAS dataset (.sas7bdat) file that is being converted to a Polars DataFrame
#         chunksize (int, optional): The number of rows to process at one time, per chunk. Defaults to 10,000.
#         processes (int, optional): The number of processes/CPU cores to use for parallel processing. Defaults to 1/4 of machines available CPU cores.
#         use_lazy (bool, optional): Whether or not to use Polar's lazy loading. Defaults to True.
#         unordered (bool, optional): Whether or not to process and read in chunk without regard to row order. Defaults to False.

#     Returns:
#         pl.DataFrame: A Polars DataFrame with the data from the SAS dataset. This may return an empty DataFrame if the SAS dataset is empty.
#     """

#     # Validate number of processes used to prevent excessive system overhead.
#     validate_processes_count(num_processes=processes)

#     # Validate input file before processing
#     if not os.path.isfile(filepath):
#         raise FileNotFoundError(f"File not found: {filepath}")
#     if not filepath.endswith(".sas7bdat"):
#         raise ValueError(f"Unsupported file type: {filepath}. Expected a .sas7bdat file.")

#     # Read metadata only to determine total row count for parallel processing.
#     _, meta = pyreadstat.read_sas7bdat(filepath, metadataonly=True)
#     total_rows = meta.number_rows

#     # Check whether inputted dataset is empty or not.
#     if total_rows == 0:
#         df, _ = pyreadstat.read_sas7bdat(filepath)
#         print("Warning: SAS dataset was empty. Returning an empty Polars DataFrame.")
#         return pl.from_pandas(df)

#     # Build args tuples for read_sas_chunk helper function.
#     args = [
#         (filepath, start, min(chunksize, total_rows - start))
#         for start in range(0, total_rows, chunksize)
#     ]
 
#     dfs: List[pl.DataFrame] = []
#     with mp.Pool(processes=processes) as pool:
#         if unordered:
#             for df in pool.imap_unordered(read_sas_chunk, args, chunksize=1):
#                 dfs.append(df)
#         else:
#             dfs = pool.starmap(read_sas_chunk, args)

#     # Only accept returned objects if they are a Polars DataFrame. 
#     dfs = [df for df in dfs if isinstance(df, pl.DataFrame)]

#     # Return empty DataFrame if no valid chunks were processed.
#     if not dfs:
#         print("Unable to successfully read in any of the data. Returning an empty Polars DataFrame.")
#         return pl.DataFrame()

#     # If use_lazy = True, apply lazy loading for memory optimization.
#     if use_lazy:
#         if hasattr(pl, "concat_lazy"): # check if current Polars version has concat_lazy method.
#             lazy_df = pl.concat_lazy([df.lazy() for df in dfs])
#         else:
#             lazy_df = pl.concat([df.lazy() for df in dfs])
#         return lazy_df.collect()

#     return pl.concat(dfs)

# def read_sas_polars(filepath: str, encoding: str = "latin-1", use_cols = None) -> pl.DataFrame:
#     """
#     Read a SAS file into a Polars DataFrame.
#     This function will attempt to read the file using a dedicated sas reader first.
#     If the user provides a usecols list, it will read the file using the usecols list.
    
#     """

#     return df


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
                   index: bool = False, indent: int = 4):
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
        xpath (str): XPath string to parse specific nodes. Default is ./*
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
                  attr_cols: list = None):
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
