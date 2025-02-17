import configparser
import os
import glob
import sys
import pandas as pd
import requests

def list_files_of_extension(directory: str, extn: str) -> list[str]:
    """
    List all files in the specified directory with the given extension.

    :param directory: The directory to search in.
    :param extn: The file extension to filter by.
    :return: A list of file paths with the specified extension.
    """
    return glob.glob(os.path.join(directory, "*." + extn))

def get_file_extension(filepath: str) -> str:
    """
    Get the file extension of the given file path.

    :param filepath: The path of the file.
    :return: The file extension of the file.
    """
    _, file_extension = os.path.splitext(filepath)
    return file_extension

def write_df(df, filepath, index=False):
    format = get_file_extension(filepath)
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

def read_df(filepath):
    format = get_file_extension(filepath)
    if format == "csv":
        return pd.read_csv(filepath)
    elif format in ("xlsx", "xls"):
        return pd.read_excel(filepath)
    elif format == "parquet":
        return pd.read_parquet(filepath)
    elif format == "psv":
        return read_psv(filepath)
    else:
        raise ValueError

def read_psv(path):
    return pd.read_csv(path, delimiter='|')

def check_folder_in_filepath(path):
    # Get the directory name of the path
    dir_name = os.path.dirname(path)
    
    if dir_name == "":
        #print("No directory specified in the path.")
        return False
    else:
        # Check if the path has a file extension
        file_extension = get_file_extension(dir_name)
        if file_extension:
            #print(f"The path '{path}' appears to be a file.")
            return False
        else:
            #print(f"The path '{path}' does not appear to have a file extension.")
            return True

def remove_directory(dir_path):
    try:
        os.rmdir(dir_path)
        print(f"Remved directory: {dir_path}")
    except Exception as e:
        print(f"Error removing directory {dir_path}: {e}")
        
def create_filepath_dirs(path):
    if check_folder_in_filepath(path):
        path = os.path.dirname(path)
        os.makedirs(path, exist_ok=True)

def download_file_from_url(url: str, save_path: str) -> None:
    """
    Downloads a file from the given URL and saves it to the specified path.

    Args:
        url (str): The URL of the file to download.
        save_path (str): The file path where the downloaded file will be saved.

    Returns:
        None
    """
    # Send a GET request to the URL
    response = requests.get(url)
    # Ensure that the directory structure exists for the save_path
    create_filepath_dirs(save_path)
    # Write the content of the response to the file in binary mode
    with open(save_path, 'wb') as file:
        file.write(response.content)

def read_file_to_string(file_path: str) -> str:
    """
    Read the string content from the specified file.

    Parameters:
    file_path (str): The path to the file.

    Returns:
    str: The content of the file as a string.
    """
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
    return content

def read_file_to_bytes(file_path: str) -> bytes:
    """
    Read the string content from the specified file and convert it to bytes using UTF-8 encoding.

    Parameters:
    file_path (str): The path to the file.

    Returns:
    bytes: The content of the file as bytes.
    """
    content = read_file_to_string(file_path)
    return content.encode('utf-8')


#read the config file
def read_ini_file(file_path):
    config = configparser.ConfigParser()
    config.read(file_path)
    
    variables = {}
    for section in config.sections():
        for key, value in config.items(section):
            variables[key] = value
    
    return variables

def addsyspath(directory: str) -> None:
    """
    Add the specified directory to the system path if it is not already included.

    Parameters:
    directory (str): The directory to be added to the system path.

    Returns:
    None
    """
    if directory not in sys.path:
        sys.path.append(directory)

def select_dataset_ui(directory: str, extension: str) -> str:
    """
    List the files with the specified extension in the given directory and prompt the user to select one.

    Parameters:
    directory (str): The directory to search for files.
    extension (str): The file extension to filter by.

    Returns:
    str: The filename of the selected dataset.
    """
    files = list_files_of_extension(directory, extension)
    for i, filename in enumerate(files):
        print(f"{i+1}. {filename}")
    selected = int(input("Enter the number of the dataset you want to select: ")) - 1
    return files[selected]
