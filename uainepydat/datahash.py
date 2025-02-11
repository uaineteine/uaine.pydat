import hmac
import hashlib

def hashhmac(datastr, sha_salt, method=hashlib.sha256): 
    hmac_object = hmac.new(sha_salt, datastr.encode('utf-8'), method)
    return hmac_object.hexdigest()

def hash256(datastr, sha_salt): 
    combined = (datastr + sha_salt).encode('utf-8')
    # Create a SHA-256 hash object
    sha256 = hashlib.sha256()
    # Update the hash object with the combined message and salt
    sha256.update(combined)
    
    return sha256.hexdigest() #output hex
