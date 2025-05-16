import hmac
import hashlib
import secrets

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

# Function to randomize a hash using hash256 with a random salt
def randomize_hash(hash_string: str, salt_length: int = 16) -> str:
    """
    Randomizes a hash by using hash256 with a random salt.
    
    Args:
        hash_string (str): The original hash or string to randomize.
        salt_length (int): The length of the random salt in bytes (default is 16).
        
    Returns:
        str: A new randomized hash derived from the original using hash256.
    """
    # Generate a random salt as a hexadecimal string
    random_salt = secrets.token_hex(salt_length)
    # Use the existing hash256 function with the random salt
    return hash256(hash_string, random_salt)

def hashmd5(datastr: str, sha_salt: str) -> str:
    """
    Create an MD5 hash using the provided data and salt.
    
    Args:
        datastr (str): The data to be hashed.
        sha_salt (str): The salt to be used in the MD5 hashing.

    Returns:
        str: The hexadecimal digest of the MD5 hash.
    """
    combined_data = (sha_salt + datastr).encode('utf-8')  # Incorporate the salt into the data
    md5_hash = hashlib.md5(combined_data).hexdigest()
    return md5_hash

# Test executions
# if __name__ == "__main__":
#     # Test data
#     test_data = "Hello, World!"
    
#     # Test hashhmac function
#     print("=== Testing hashhmac function ===")
#     hmac_salt = b"test_salt"
#     hmac_result = hashhmac(test_data, hmac_salt)
#     print(f"Data: {test_data}")
#     print(f"Salt: {hmac_salt}")
#     print(f"HMAC Hash: {hmac_result}")
#     print()
    
#     # Test hash256 function
#     print("=== Testing hash256 function ===")
#     hash_salt = "test_salt"
#     hash_result = hash256(test_data, hash_salt)
#     print(f"Data: {test_data}")
#     print(f"Salt: {hash_salt}")
#     print(f"SHA-256 Hash: {hash_result}")
#     print()
    
#     # Test randomize_hash function
#     print("=== Testing randomize_hash function ===")
#     print(f"Original string: {test_data}")
#     # Generate multiple random hashes to demonstrate randomness
#     print("Random hashes:")
#     for i in range(3):
#         random_hash = randomize_hash(test_data)
#         print(f"  Random hash {i+1}: {random_hash}")
    
#     # Demonstrate that hashing the same string twice produces different results
#     print("\nHashing the same string twice:")
#     hash1 = randomize_hash("Same input")
#     hash2 = randomize_hash("Same input")
#     print(f"  First hash:  {hash1}")
#     print(f"  Second hash: {hash2}")
#     print(f"  Hashes are {'different' if hash1 != hash2 else 'the same'}")
