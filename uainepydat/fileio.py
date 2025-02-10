import os
import pandas as pd
import requests

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
        
def create_filepath_dirs(path):
    if check_folder_in_filepath(path):
        path = os.path.dirname(path)
        os.makedirs(path, exist_ok=True)

def download_file_from_url(url, save_path):
    response = requests.get(url)
	create_filepath_dirs(save_path)
    with open(save_path, 'wb') as file:
        file.write(response.content)

def read_file_to_bytes(file_path):
    # Read the string content from the file
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
    # Convert the string content to bytes using UTF-8 
    return content.encode('utf-8')
