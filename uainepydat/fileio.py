import os
import glob
import sys

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
