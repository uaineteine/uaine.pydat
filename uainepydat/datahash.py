import hmac
import hashlib

def hash256hmac(datastr, sha_salt): 
    hmac_object = hmac.new(sha_salt, datastr.encode('utf-8'), hashlib.sha256)
    return hmac_object.hexdigest()

def hash256(datastr, sha_salt): 
    hmac_object = hmac.new(sha_salt, datastr.encode('utf-8'), hashlib.sha256)
    return hmac_object.hexdigest()
