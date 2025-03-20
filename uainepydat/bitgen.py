import numpy as np
import uuid

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

def generate_random_uuid() -> str:
    """
    Generates a random UUID (Universally Unique Identifier) using UUID version 4,
    with dashes removed.

    Returns:
        str: A string representation of a random UUID without dashes.
    """
    return str(uuid.uuid4()).replace('-', '')

def generate_custom_uuid(marker: str = '-') -> str:
    """
    Generates a random UUID (Universally Unique Identifier) using UUID version 4,
    formatted with a specified marker.

    Parameters:
        marker (str): The character to use as a separator in the UUID. Default is '-'.

    Returns:
        str: A string representation of a random UUID with the specified marker.
    """
    # Generate a UUID without dashes
    uuid_str = generate_random_uuid()
    
    # Define the size of each group
    group_size = 8
    
    # Format the UUID with the specified marker
    formatted_uuid = marker.join(
        uuid_str[i:i + group_size] for i in range(0, len(uuid_str), group_size)
    )
    
    return formatted_uuid

# Example usage
#random_uuid = generate_random_uuid()
#print(random_uuid)

#custom_uuid = generate_custom_uuid('.')
#print(custom_uuid)
