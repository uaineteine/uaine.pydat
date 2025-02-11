import configparser
import os
import sys
import pandas as pd
import requests

def get_file_extension(filepath):
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

def download_file_from_url(url, save_path):
    response = requests.get(url)
    create_filepath_dirs(save_path)
    with open(save_path, 'wb') as file:
        file.write(response.content)

def read_file_to_string(file_path):
    # Read the string content from the file
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
    # Convert the string content to bytes using UTF-8 
    return content.encode('utf-8')

def read_file_to_bytes(file_path):
    content = read_file_to_string(file_path)
    # Convert the string content to bytes using UTF-8 
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

def addsyspath(directory):
    if directory not in sys.path:
        sys.path.append(directory)
