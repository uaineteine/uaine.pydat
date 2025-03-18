import os
import glob
import subprocess
import sys
import uuid
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
    bn = os.path.basename(filepath)
    _, file_extension = os.path.splitext(bn)
    return file_extension[1:]  # Remove the leading dot

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

def remove_directory(dir_path: str) -> bool:
    """
    Removes a directory at the specified path.
    
    Attempts to remove the directory and prints the result. If an error occurs
    during removal, the exception is caught and an error message is printed.
    
    Parameters:
        dir_path (str): The path to the directory to be removed.
        
    Returns:
        bool: True if directory was successfully removed, False otherwise.
        
    Raises:
        No exceptions are raised as they are caught and printed internally.
    """
    try:
        os.rmdir(dir_path)
        print(f"Removed directory: {dir_path}")
        return True
    except Exception as e:
        print(f"Error removing directory {dir_path}: {e}")
        return False
        
def create_filepath_dirs(path: str) -> None:
    """
    Creates all directories needed for a given file path.
    
    If the path contains folders, this function creates all necessary 
    directories in the path if they don't already exist.
    
    Parameters:
        path (str): The file path for which to create directories.
        
    Returns:
        None
    """
    if check_folder_in_filepath(path):
        path = os.path.dirname(path)
        os.makedirs(path, exist_ok=True)

def download_file_from_url(url: str, save_path: str):
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

def addsyspath(directory: str):
    """
    Add the specified directory to the system path if it is not already included.

    Parameters:
    directory (str): The directory to be added to the system path.

    Returns:
    None
    """
    if directory not in sys.path:
        sys.path.append(directory)

def mv_file(src: str, dest: str):
    """
    Moves a file from the source path to the destination path.
    
    Parameters:
    src (str): The path of the file to be moved.
    dest (str): The destination path where the file should be moved.
    
    Returns:
    None
    """
    subprocess.run(["mv", src, dest])

def gen_random_subfolder(master_dir: str) -> str:
    """
    Generates a random subfolder within the specified master directory.

    Args:
        master_dir (str): The path to the master directory where the subfolder will be created.

    Returns:
        str: The path to the newly created subfolder.
    """
    guid = str(uuid.uuid4())  # Generate a unique identifier for the subfolder
    out_folder = os.path.join(master_dir, guid)  # Output folder location
    create_filepath_dirs(out_folder)  # Ensure the directory structure exists
    return out_folder

def list_dirs(main_dir: str) -> list:
    """
    List all directories within the specified main directory.

    Args:
    main_dir (str): The main directory path to list directories from.

    Returns:
    list: A list of directory names within the specified main directory.
    """
    lsall = os.listdir(main_dir)
    lsd = [dir for dir in lsall if os.path.isdir(os.path.join(main_dir, dir))]
    return lsd
