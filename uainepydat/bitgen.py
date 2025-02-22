import numpy as np

def gen_random_hex_string(bitlength) -> str:
    """
    Generates a random hexadecimal string of the given bit length.

    Parameters:
    bitlength (int): The length of the bit string to generate.

    Returns:
    str: A random hexadecimal string of the given bit length.
    """
    return ''.join(np.random.choice(list('0123456789abcdef'), size=int(bitlength/4)))

def generate_256_bit_string() -> str:
    """
    Generates a random 256-bit hexadecimal string.

    Returns:
    str: A random 256-bit hexadecimal string.
    """
    return gen_random_hex_string(256)

def generate_8_bit_string() -> str:
    """
    Generates a random 8-bit hexadecimal string.

    Returns:
    str: A random 8-bit hexadecimal string.
    """
    return gen_random_hex_string(8)
