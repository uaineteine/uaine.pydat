import hmac
import hashlib

# Function to create an HMAC hash
def hashhmac(datastr: str, sha_salt: bytes, method=hashlib.sha256) -> str:
    """
    Create an HMAC hash using the provided data, salt, and method.
    
    Args:
        datastr (str): The data to be hashed.
        sha_salt (bytes): The salt to be used in the HMAC hashing.
        method: The hashing method to be used (default is hashlib.sha256).

    Returns:
        str: The hexadecimal digest of the HMAC hash.
    """
    # Create a new HMAC object using the provided data, salt, and method
    hmac_object = hmac.new(sha_salt, datastr.encode('utf-8'), method)
    # Return the hexadecimal digest of the HMAC object
    return hmac_object.hexdigest()

# Function to create a SHA-256 hash
def hash256(datastr: str, sha_salt: str) -> str:
    """
    Create a SHA-256 hash using the provided data and salt.
    
    Args:
        datastr (str): The data to be hashed.
        sha_salt (str): The salt to be used in the SHA-256 hashing.

    Returns:
        str: The hexadecimal digest of the SHA-256 hash.
    """
    # Combine the data string and salt into a single byte string
    combined = (datastr + sha_salt).encode('utf-8')
    # Create a new SHA-256 hash object
    sha256 = hashlib.sha256()
    # Update the hash object with the combined byte string
    sha256.update(combined)
    # Return the hexadecimal digest of the SHA-256 hash
    return sha256.hexdigest()
