import os
from tqdm import tqdm
from azure.storage.blob import BlobServiceClient

def get_account_url(storage_account):
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
    blobs = list_blob_content(account_url, container, folder_path, sastoken, file_extn=file_extn)

    os.makedirs(download_loc, exist_ok=True)
    
    blob_serv_client = BlobServiceClient(account_url=account_url, credential=sastoken)
    cont_client = blob_serv_client.get_container_client(container)
    for blob in tqdm(blobs, desc="Downloading", unit="file"):
        blob_client = cont_client.get_blob_client(blob.name)
        down_path = os.path.join(download_loc, os.path.basename(blob.name))
    
        with open(down_path, "wb") as file:
            file.write(blob_client.download_blob().readall())

def test1():
    """
    Example usage and test suite for blob helper functions.
    """
    import sys
    
    # Example usage
    storage_account = "yourstorageaccount"
    container = "yourcontainer"
    subfolder = "folder1/folder2"
    print(get_blob_subfolder_path(storage_account, container, subfolder))
    
    def run_test(test_func, name):
        """Helper function to run and report test results"""
        try:
            test_func()
            print(f"✓ {name} passed")
            return True
        except AssertionError as e:
            print(f"✗ {name} failed: {e}")
            return False
    
    def test_get_blob_container_path():
        """Test the get_blob_container_path function"""
        # Test case 1: Basic functionality
        result = get_blob_container_path("teststorage", "testcontainer")
        expected = "https://teststorage.blob.core.windows.net/testcontainer"
        assert result == expected, f"Expected '{expected}', got '{result}'"
        
        # Test case 2: Special characters in storage account name
        result = get_blob_container_path("test-storage", "testcontainer")
        expected = "https://test-storage.blob.core.windows.net/testcontainer"
        assert result == expected, f"Expected '{expected}', got '{result}'"
        
        # Test case 3: Special characters in container name
        result = get_blob_container_path("teststorage", "test-container")
        expected = "https://teststorage.blob.core.windows.net/test-container"
        assert result == expected, f"Expected '{expected}', got '{result}'"
    
    def test_get_blob_subfolder_path():
        """Test the get_blob_subfolder_path function"""
        # Test case 1: Basic functionality
        result = get_blob_subfolder_path("teststorage", "testcontainer", "testfolder")
        expected = "https://teststorage.blob.core.windows.net/testcontainer/testfolder"
        assert result == expected, f"Expected '{expected}', got '{result}'"
        
        # Test case 2: Nested subfolder path
        result = get_blob_subfolder_path("teststorage", "testcontainer", "folder1/folder2")
        expected = "https://teststorage.blob.core.windows.net/testcontainer/folder1/folder2"
        assert result == expected, f"Expected '{expected}', got '{result}'"
        
        # Test case 3: Subfolder with special characters
        result = get_blob_subfolder_path("teststorage", "testcontainer", "folder-with-dashes")
        expected = "https://teststorage.blob.core.windows.net/testcontainer/folder-with-dashes"
        assert result == expected, f"Expected '{expected}', got '{result}'"
        
        # Test case 4: Empty subfolder path
        result = get_blob_subfolder_path("teststorage", "testcontainer", "")
        expected = "https://teststorage.blob.core.windows.net/testcontainer/"
        assert result == expected, f"Expected '{expected}', got '{result}'"
    
    # Run all tests
    print("Running tests for blobcontainer.py module...")
    tests = [
        (test_get_blob_container_path, "get_blob_container_path"),
        (test_get_blob_subfolder_path, "get_blob_subfolder_path")
    ]
    
    failed = 0
    for test_func, name in tests:
        if not run_test(test_func, name):
            failed += 1
    
    # Report test results
    total = len(tests)
    passed = total - failed
    print(f"\nTest results: {passed}/{total} tests passed")
    
    # Set exit code based on test results
    sys.exit(1 if failed > 0 else 0)

def test2():
    """
    Test suite for blob operations using Azure Storage Emulator.
    Tests listing and downloading blobs from the emulator.
    """
    import sys
    
    # Azure Storage Emulator settings
    account_url = "http://127.0.0.1:10000/devstoreaccount1"
    container = "test-container"
    sastoken = "?sv=2018-03-28&st=2025-06-15T10%3A36%3A47Z&se=2025-06-16T10%3A36%3A47Z&sr=c&sp=rl&sig=FzDNsOBYgBTXALSnaMFbTAludECRsg0uzA4ihhBp2V0%3D"  # Example token
    test_folder = "test-folder"
    download_loc = "test-downloads"
    
    def run_test(test_func, name):
        """Helper function to run and report test results"""
        try:
            test_func()
            print(f"✓ {name} passed")
            return True
        except Exception as e:
            print(f"✗ {name} failed: {str(e)}")
            return False
    
    def test_list_blobs():
        """Test listing blobs from the container"""
        try:
            blobs = list_blob_content(account_url, container, test_folder, sastoken)
            # Convert to list to check if we can iterate
            blob_list = list(blobs)
            print(f"Found {len(blob_list)} blobs in {test_folder}")
            return True
        except Exception as e:
            raise AssertionError(f"Failed to list blobs: {str(e)}")
    
    def test_download_blobs():
        """Test downloading blobs from the container"""
        try:
            download_all_blobs(account_url, container, test_folder, sastoken, download_loc)
            # Check if download directory was created
            assert os.path.exists(download_loc), f"Download directory {download_loc} was not created"
            return True
        except Exception as e:
            raise AssertionError(f"Failed to download blobs: {str(e)}")
    
    # Run all tests
    print("\nRunning tests for blob operations using Azure Storage Emulator...")
    tests = [
        (test_list_blobs, "list_blobs"),
        (test_download_blobs, "download_blobs")
    ]
    
    failed = 0
    for test_func, name in tests:
        if not run_test(test_func, name):
            failed += 1
    
    # Report test results
    total = len(tests)
    passed = total - failed
    print(f"\nTest results: {passed}/{total} tests passed")
    
    # Cleanup
    if os.path.exists(download_loc):
        import shutil
        shutil.rmtree(download_loc)
    
    # Set exit code based on test results
    sys.exit(1 if failed > 0 else 0)

if __name__ == "__main__":
    #test1()
    test2()
