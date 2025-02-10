import os
import pandas as pd
import requests

def write_df(df, filepath, index=False):
    format = filepath.split(".")[-1]
    if format == "csv":
        df.to_csv(filepath, chunksize=50000, index=index)
    elif format in ("xlsx", "xls"):
        df.to_excel(filepath, index=index)
    elif format == "parquet":
        df.to_parquet(filepath, index=index)
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
        _, file_extension = os.path.splitext(dir_name)
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