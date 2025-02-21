import os
import glob
import sys
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

def download_file_from_url(url: str, save_path: str)
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

def addsyspath(directory: str)
    """
    Add the specified directory to the system path if it is not already included.

    Parameters:
    directory (str): The directory to be added to the system path.

    Returns:
    None
    """
    if directory not in sys.path:
        sys.path.append(directory)
