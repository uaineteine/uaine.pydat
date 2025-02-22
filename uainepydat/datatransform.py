def replace_between_tags(content: str, tag_name: str, new_lines: list[str], deleteTags=False) -> str:
    start_tag = f'<{tag_name}>'
    end_tag = f'</{tag_name}>'
    
    start_index = content.find(start_tag)
    end_index = content.find(end_tag, start_index)

    if start_index == -1 or end_index == -1:
        raise ValueError("Tags not found in the content")

    #delete tags themeselves by modifying the selection index
    if deleteTags:
        new_content = content[:start_index] + '\n'.join(new_lines) + content[end_index + len(end_tag):]
    else:
        new_content = content[:start_index + len(start_tag)] + '\n' + '\n'.join(new_lines) + '\n' + content[end_index:]

    return new_content

def break_into_lines(string: str) -> list[str]:
    """
    Breaks a string into a list of lines.

    Args:
        string (str): The input string to be broken into lines.

    Returns:
        list[str]: A list of lines from the input string.
    """
    return string.split('\n')


def add_prefix(string: str, prefix: str) -> str:
    """
    Add the specified prefix to the string.

    Parameters:
    string (str): The original string.
    prefix (str): The prefix to add to the string.

    Returns:
    str: The string with the prefix added.
    """
    return prefix + string

def add_suffix(string: str, suffix: str) -> str:
    """
    Add the specified suffix to the string.

    Parameters:
    string (str): The original string.
    suffix (str): The suffix to add to the string.

    Returns:
    str: The string with the suffix added.
    """
    return string + suffix

import numpy as np

def gen_random_hex_string(bitlength):
    """
    Generates a random hexadecimal string of the given bit length.

    Parameters:
    bitlength (int): The length of the bit string to generate.

    Returns:
    str: A random hexadecimal string of the given bit length.
    """
    return ''.join(np.random.choice(list('0123456789abcdef'), size=int(bitlength/4)))

def generate_256_bit_string():
    """
    Generates a random 256-bit hexadecimal string.

    Returns:
    str: A random 256-bit hexadecimal string.
    """
    return gen_random_hex_string(256)

def generate_8_bit_string():
    """
    Generates a random 8-bit hexadecimal string.

    Returns:
    str: A random 8-bit hexadecimal string.
    """
    return gen_random_hex_string(8)
