import os
from tqdm import tqdm
from azure.storage.blob import BlobServiceClient, BlobClient
import base64

def get_account_url(storage_account):
    """
    Generate the Azure Storage account URL from the storage account name.

    Args:
        storage_account (str): The name of the Azure Storage account.

    Returns:
        str: The complete Azure Storage account URL in the format 
             'https://{storage_account}.blob.core.windows.net'

    Example:
        >>> get_account_url('myaccount')
        'https://myaccount.blob.core.windows.net'
    """
    return f"https://{storage_account}.blob.core.windows.net"

def get_blob_container_path(storage_account, container):
    """
    Returns the absolute path of the blob container.

    :param storage_account: Name of the Azure Storage account.
    :param container: Name of the blob container.
    :return: Absolute path as a string.
    """
    account_url = get_account_url(storage_account)
    #return f"{https://{storage_account}.blob.core.windows.net}/{container}"
    return f"{account_url}/{container}"

def get_blob_subfolder_path(storage_account, container, subfolder):
    """
    Returns the absolute path of a blob subfolder using the container path function.

    :param storage_account: Name of the Azure Storage account.
    :param container: Name of the blob container.
    :param subfolder: Path to subfolder within the container.
    :return: Absolute path as a string.
    """
    base_path = get_blob_container_path(storage_account, container)
    return f"{base_path}/{subfolder}"

def list_blob_content(account_url, container, folder_path, sastoken, file_extn = ""):
    """
    List blobs in an Azure Storage container with optional file extension filtering.

    Args:
        account_url (str): The Azure Storage account URL.
        container (str): The name of the container to list blobs from.
        folder_path (str): The folder path prefix to filter blobs by.
        sastoken (str): The SAS token for authentication.
        file_extn (str, optional): File extension to filter blobs by (e.g., 'txt', 'pdf'). 
                                 If empty, returns all blobs. Defaults to "".

    Returns:
        list: A list of BlobProperties objects matching the specified criteria.
    """
    blob_serv_client = BlobServiceClient(account_url=account_url, credential=sastoken)
    cont_client = blob_serv_client.get_container_client(container)
    blobs = cont_client.list_blobs(name_starts_with=folder_path)

    #apply type filtering for file extensions
    if file_extn != "":
        tar_blobs = [blob for blob in blobs if blob.name.endswith("." + file_extn)]
        return tar_blobs
    #implied else
    return blobs

def download_all_blobs(account_url, container, folder_path, sastoken, download_loc, file_extn="", makedirs=True):
    """
    Download all blobs from an Azure Storage container to a local directory.

    Args:
        account_url (str): The Azure Storage account URL.
        container (str): The name of the container to download blobs from.
        folder_path (str): The folder path prefix to filter blobs by.
        sastoken (str): The SAS token for authentication.
        download_loc (str): Local directory path where blobs will be downloaded.
        file_extn (str, optional): File extension to filter blobs by (e.g., 'txt', 'pdf'). 
                                 If empty, downloads all blobs. Defaults to "".
        makedirs (bool, optional): Whether to create the download directory if it doesn't exist. 
                                 Defaults to True.

    Note:
        This function will create the download directory if it doesn't exist and makedirs is True.
        Files are downloaded with their original names from the blob storage.
    """
    blobs = list_blob_content(account_url, container, folder_path, sastoken, file_extn=file_extn)

    os.makedirs(download_loc, exist_ok=makedirs)
    
    blob_serv_client = BlobServiceClient(account_url=account_url, credential=sastoken)
    cont_client = blob_serv_client.get_container_client(container)
    for blob in tqdm(blobs, desc="Downloading", unit="file"):
        blob_client = cont_client.get_blob_client(blob.name)
        down_path = os.path.join(download_loc, os.path.basename(blob.name))
    
        with open(down_path, "wb") as file:
            file.write(blob_client.download_blob().readall())

def download_all_blobs_in_chunks(account_url, container, folder_path, sastoken, download_loc, 
                                 file_extn="", makedirs=True, chunk_size=16 * 1024 * 1024):
    blobs = list(list_blob_content(account_url, container, folder_path, sastoken, file_extn=file_extn))
    print(f"Number of blobs found: {len(blobs)}")
    os.makedirs(download_loc, exist_ok=makedirs)

    blob_serv_client = BlobServiceClient(account_url=account_url, credential=sastoken)
    cont_client = blob_serv_client.get_container_client(container)

    # Precompute total download size
    total_bytes = sum(blob.size for blob in blobs)
    print(f"Downloading {round(total_bytes/1024/1024, 2)} MB of data")

    with tqdm(total=total_bytes, unit='B', unit_scale=True, unit_divisor=1024, desc="Total Download") as total_bar:
        for blob in blobs:
            blob_client = cont_client.get_blob_client(blob.name)
            down_path = os.path.join(download_loc, os.path.basename(blob.name))
            print(f"Downloading to: {down_path}")
            try:
                with open(down_path, "wb") as file:
                    stream = blob_client.download_blob()
                    for chunk in stream.chunks(chunk_size):
                        file.write(chunk)
                        total_bar.update(chunk_size)
            except Exception as e:
                print(f"Failed to download {blob.name}: {e}")

def get_blob_md5_checksums(account_url, container, sastoken, blob_list, use_hex=False):
    """
    Retrieves MD5 checksums for a list of blobs in Azure Blob Storage.

    Args:
        account_url (str): The Azure Storage account URL.
        container (str): The name of the container.
        sastoken (str): The SAS token for authentication.
        blob_list (list): List of BlobProperties objects.
        use_hex (bool): If True, returns checksum as hex; otherwise Base64. Default is False.

    Returns:
        dict: A dictionary mapping blob names to their MD5 checksums, or None if not available.
    """
    checksums = {}
    for blob in tqdm(blob_list, desc="Fetching checksums", unit="file"):
        blob_client = BlobClient(account_url=account_url, container_name=container,
                                blob_name=blob.name, credential=sastoken)
        props = blob_client.get_blob_properties()
        md5 = props.content_settings.content_md5
        if md5:
            checksums[blob.name] = md5.hex() if use_hex else base64.b64encode(md5).decode('utf-8')
        else:
            checksums[blob.name] = None
    return checksums

# def test3():
#     """
#     Test suite for chunked blob downloads using Azure Storage Emulator or a test account.
#     Tests downloading blobs in chunks and verifies the download directory and files.
#     """
#     import sys
#     import glob
#     import shutil

#     # Azure Storage Emulator or test account settings
#     account_url = "http://127.0.0.1:10000/devstoreaccount1"  # Change as needed
#     container = "test-container"
#     sastoken = "?sv=2018-03-28&st=2025-06-20T06%3A25%3A16Z&se=2025-06-21T06%3A25%3A16Z&sr=c&sp=rl&sig=2Ds12w6B2h1hyP5VXFup%2BEZ16%2BvV3J3A3F%2BXtoYoyyM%3D"  # Example token
#     test_folder = "test-folder"
#     download_loc = "test-downloads-chunks"
#     file_extn = ""  # Set to a specific extension if desired

#     def run_test(test_func, name):
#         try:
#             test_func()
#             print(f"✓ {name} passed")
#             return True
#         except Exception as e:
#             print(f"✗ {name} failed: {str(e)}")
#             return False

#     def test_chunked_download():
#         # try:
#             # Download blobs in chunks
#             download_all_blobs_in_chunks(account_url, container, test_folder, sastoken, download_loc, file_extn=file_extn, makedirs=True)
#             print("assessing test")
#             # Check if download directory was created
#             assert os.path.exists(download_loc), f"Download directory {download_loc} was not created"
#             # Check if at least one file was downloaded
#             files = glob.glob(os.path.join(download_loc, "*"))
#             assert len(files) > 0, f"No files were downloaded to {download_loc}"
#             print(f"Downloaded {len(files)} files to {download_loc}")
#         # except Exception as e:
#         #     raise AssertionError(f"Failed to download blobs in chunks: {str(e)}")

#     # Run the test
#     print("\nRunning test for chunked blob downloads...")
#     tests = [
#         (test_chunked_download, "chunked_download")
#     ]
#     failed = 0
#     for test_func, name in tests:
#         if not run_test(test_func, name):
#             failed += 1
#     total = len(tests)
#     passed = total - failed
#     print(f"\nTest results: {passed}/{total} tests passed")

#     # Cleanup
#     if os.path.exists(download_loc):
#         shutil.rmtree(download_loc)

#     sys.exit(1 if failed > 0 else 0)

def test4():
    """
    Test suite for blob MD5 checksums using Azure Storage Emulator or a test account.
    Tests retrieving MD5 checksums for blobs and verifies the checksum format.
    """
    import sys
    import glob
    import shutil

    # Azure Storage Emulator or test account settings
    account_url = "http://127.0.0.1:10000/devstoreaccount1"  # Change as needed
    container = "test-container"
    sastoken = "?sv=2018-03-28&st=2025-06-21T04%3A02%3A10Z&se=2025-06-22T04%3A02%3A10Z&sr=c&sp=rl&sig=l1PpnTCSVoW3dd1os%2FH7GR3t6%2FrTDOz%2FSERcwPhTA%2BU%3D"  # Example token
    test_folder = "test-folder"

    def run_test(test_func, name):
        try:
            test_func()
            print(f"✓ {name} passed")
            return True
        except Exception as e:
            print(f"✗ {name} failed: {str(e)}")
            return False

    def test_get_checksums_base64():
        """Test retrieving MD5 checksums in Base64 format"""
        try:
            # First, list blobs to get blob properties
            blobs = list_blob_content(account_url, container, test_folder, sastoken)
            blob_list = list(blobs)
            
            if len(blob_list) == 0:
                print("No blobs found for testing checksums")
                return True
            
            # Get checksums in Base64 format (default)
            checksums = get_blob_md5_checksums(account_url, container, sastoken, blob_list, use_hex=False)
            
            # Verify checksums dictionary structure
            assert isinstance(checksums, dict), "Checksums should be returned as a dictionary"
            assert len(checksums) == len(blob_list), "Number of checksums should match number of blobs"
            
            # Check that all blob names are present as keys
            for blob in blob_list:
                assert blob.name in checksums, f"Blob {blob.name} should have a checksum entry"
            
            # Verify checksum format (Base64 should be a string)
            for blob_name, checksum in checksums.items():
                if checksum is not None:  # Some blobs might not have MD5
                    assert isinstance(checksum, str), f"Checksum for {blob_name} should be a string"
                    # Base64 strings should only contain valid Base64 characters
                    import re
                    assert re.match(r'^[A-Za-z0-9+/]*={0,2}$', checksum), f"Checksum for {blob_name} should be valid Base64"
            
            print(f"Successfully retrieved {len(checksums)} checksums in Base64 format")
            return True
            
        except Exception as e:
            raise AssertionError(f"Failed to get checksums in Base64 format: {str(e)}")

    def test_get_checksums_hex():
        """Test retrieving MD5 checksums in hex format"""
        try:
            # First, list blobs to get blob properties
            blobs = list_blob_content(account_url, container, test_folder, sastoken)
            blob_list = list(blobs)
            
            if len(blob_list) == 0:
                print("No blobs found for testing checksums")
                return True
            
            # Get checksums in hex format
            checksums = get_blob_md5_checksums(account_url, container, sastoken, blob_list, use_hex=True)
            
            # Verify checksums dictionary structure
            assert isinstance(checksums, dict), "Checksums should be returned as a dictionary"
            assert len(checksums) == len(blob_list), "Number of checksums should match number of blobs"
            
            # Check that all blob names are present as keys
            for blob in blob_list:
                assert blob.name in checksums, f"Blob {blob.name} should have a checksum entry"
            
            # Verify checksum format (hex should be a 32-character hex string)
            for blob_name, checksum in checksums.items():
                if checksum is not None:  # Some blobs might not have MD5
                    assert isinstance(checksum, str), f"Checksum for {blob_name} should be a string"
                    assert len(checksum) == 32, f"Hex checksum for {blob_name} should be 32 characters long"
                    # Hex strings should only contain valid hex characters
                    import re
                    assert re.match(r'^[0-9a-f]{32}$', checksum.lower()), f"Checksum for {blob_name} should be valid hex"
            
            print(f"Successfully retrieved {len(checksums)} checksums in hex format")
            return True
            
        except Exception as e:
            raise AssertionError(f"Failed to get checksums in hex format: {str(e)}")

    def test_checksums_consistency():
        """Test that checksums are consistent between Base64 and hex formats"""
        try:
            # First, list blobs to get blob properties
            blobs = list_blob_content(account_url, container, test_folder, sastoken)
            blob_list = list(blobs)
            
            if len(blob_list) == 0:
                print("No blobs found for testing checksums")
                return True
            
            # Get checksums in both formats
            checksums_base64 = get_blob_md5_checksums(account_url, container, sastoken, blob_list, use_hex=False)
            checksums_hex = get_blob_md5_checksums(account_url, container, sastoken, blob_list, use_hex=True)

            print(checksums_base64)
            print("")
            print(checksums_hex)
            
            # Verify that both dictionaries have the same keys
            assert set(checksums_base64.keys()) == set(checksums_hex.keys()), "Both checksum formats should have the same blob keys"
            
            # For blobs that have checksums in both formats, verify they represent the same value
            for blob_name in checksums_base64.keys():
                base64_checksum = checksums_base64[blob_name]
                hex_checksum = checksums_hex[blob_name]
                
                if base64_checksum is not None and hex_checksum is not None:
                    # Convert Base64 to hex and compare
                    import base64
                    base64_bytes = base64.b64decode(base64_checksum)
                    base64_as_hex = base64_bytes.hex()
                    assert base64_as_hex == hex_checksum, f"Checksum mismatch for {blob_name}: Base64->hex conversion doesn't match hex format"
            
            print("Checksum format consistency verified")
            return True
            
        except Exception as e:
            raise AssertionError(f"Failed to verify checksum consistency: {str(e)}")

    # Run the tests
    print("\nRunning test for blob MD5 checksums...")
    tests = [
        (test_get_checksums_base64, "get_checksums_base64"),
        (test_get_checksums_hex, "get_checksums_hex"),
        (test_checksums_consistency, "checksums_consistency")
    ]
    failed = 0
    for test_func, name in tests:
        if not run_test(test_func, name):
            failed += 1
    total = len(tests)
    passed = total - failed
    print(f"\nTest results: {passed}/{total} tests passed")

    sys.exit(1 if failed > 0 else 0)

# def test1():
#     """
#     Example usage and test suite for blob helper functions.
#     """
#     import sys
#     
#     # Example usage
#     storage_account = "yourstorageaccount"
#     container = "yourcontainer"
#     subfolder = "folder1/folder2"
#     print(get_blob_subfolder_path(storage_account, container, subfolder))
#     
#     def run_test(test_func, name):
#         """Helper function to run and report test results"""
#         try:
#             test_func()
#             print(f"✓ {name} passed")
#             return True
#         except AssertionError as e:
#             print(f"✗ {name} failed: {e}")
#             return False
#     
#     def test_get_blob_container_path():
#         """Test the get_blob_container_path function"""
#         # Test case 1: Basic functionality
#         result = get_blob_container_path("teststorage", "testcontainer")
#         expected = "https://teststorage.blob.core.windows.net/testcontainer"
#         assert result == expected, f"Expected '{expected}', got '{result}'"
#         
#         # Test case 2: Special characters in storage account name
#         result = get_blob_container_path("test-storage", "testcontainer")
#         expected = "https://test-storage.blob.core.windows.net/testcontainer"
#         assert result == expected, f"Expected '{expected}', got '{result}'"
#         
#         # Test case 3: Special characters in container name
#         result = get_blob_container_path("teststorage", "test-container")
#         expected = "https://teststorage.blob.core.windows.net/test-container"
#         assert result == expected, f"Expected '{expected}', got '{result}'"
#     
#     def test_get_blob_subfolder_path():
#         """Test the get_blob_subfolder_path function"""
#         # Test case 1: Basic functionality
#         result = get_blob_subfolder_path("teststorage", "testcontainer", "testfolder")
#         expected = "https://teststorage.blob.core.windows.net/testcontainer/testfolder"
#         assert result == expected, f"Expected '{expected}', got '{result}'"
#         
#         # Test case 2: Nested subfolder path
#         result = get_blob_subfolder_path("teststorage", "testcontainer", "folder1/folder2")
#         expected = "https://teststorage.blob.core.windows.net/testcontainer/folder1/folder2"
#         assert result == expected, f"Expected '{expected}', got '{result}'"
#         
#         # Test case 3: Subfolder with special characters
#         result = get_blob_subfolder_path("teststorage", "testcontainer", "folder-with-dashes")
#         expected = "https://teststorage.blob.core.windows.net/testcontainer/folder-with-dashes"
#         assert result == expected, f"Expected '{expected}', got '{result}'"
#         
#         # Test case 4: Empty subfolder path
#         result = get_blob_subfolder_path("teststorage", "testcontainer", "")
#         expected = "https://teststorage.blob.core.windows.net/testcontainer/"
#         assert result == expected, f"Expected '{expected}', got '{result}'"
#     
#     # Run all tests
#     print("Running tests for blobcontainer.py module...")
#     tests = [
#         (test_get_blob_container_path, "get_blob_container_path"),
#         (test_get_blob_subfolder_path, "get_blob_subfolder_path")
#     ]
#     
#     failed = 0
#     for test_func, name in tests:
#         if not run_test(test_func, name):
#             failed += 1
#     
#     # Report test results
#     total = len(tests)
#     passed = total - failed
#     print(f"\nTest results: {passed}/{total} tests passed")
#     
#     # Set exit code based on test results
#     sys.exit(1 if failed > 0 else 0)

# def test2():
#     """
#     Test suite for blob operations using Azure Storage Emulator.
#     Tests listing and downloading blobs from the emulator.
#     """
#     import sys
#     
#     # Azure Storage Emulator settings
#     account_url = "http://127.0.0.1:10000/devstoreaccount1"
#     container = "test-container"
#     sastoken = "?sv=2018-03-28&st=2025-06-15T10%3A36%3A47Z&se=2025-06-16T10%3A36%3A47Z&sr=c&sp=rl&sig=FzDNsOBYgBTXALSnaMFbTAludECRsg0uzA4ihhBp2V0%3D"  # Example token
#     test_folder = "test-folder"
#     download_loc = "test-downloads"
#     
#     def run_test(test_func, name):
#         """Helper function to run and report test results"""
#         try:
#             test_func()
#             print(f"✓ {name} passed")
#             return True
#         except Exception as e:
#             print(f"✗ {name} failed: {str(e)}")
#             return False
#     
#     def test_list_blobs():
#         """Test listing blobs from the container"""
#         try:
#             blobs = list_blob_content(account_url, container, test_folder, sastoken)
#             # Convert to list to check if we can iterate
#             blob_list = list(blobs)
#             print(f"Found {len(blob_list)} blobs in {test_folder}")
#             return True
#         except Exception as e:
#             raise AssertionError(f"Failed to list blobs: {str(e)}")
#     
#     def test_download_blobs():
#         """Test downloading blobs from the container"""
#         try:
#             download_all_blobs(account_url, container, test_folder, sastoken, download_loc)
#             # Check if download directory was created
#             assert os.path.exists(download_loc), f"Download directory {download_loc} was not created"
#             return True
#         except Exception as e:
#             raise AssertionError(f"Failed to download blobs: {str(e)}")
#     
#     # Run all tests
#     print("\nRunning tests for blob operations using Azure Storage Emulator...")
#     tests = [
#         (test_list_blobs, "list_blobs"),
#         (test_download_blobs, "download_blobs")
#     ]
#     
#     failed = 0
#     for test_func, name in tests:
#         if not run_test(test_func, name):
#             failed += 1
#     
#     # Report test results
#     total = len(tests)
#     passed = total - failed
#     print(f"\nTest results: {passed}/{total} tests passed")
#     
#     # Cleanup
#     if os.path.exists(download_loc):
#         import shutil
#         shutil.rmtree(download_loc)
#     
#     # Set exit code based on test results
#     sys.exit(1 if failed > 0 else 0)

if __name__ == "__main__":
#     #test1()
#     #test2()
     test4()
