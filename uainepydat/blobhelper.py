def get_blob_container_path(storage_account, container):
    """
    Returns the absolute path of the blob container.

    :param storage_account: Name of the Azure Storage account.
    :param container: Name of the blob container.
    :return: Absolute path as a string.
    """
    return f"https://{storage_account}.blob.core.windows.net/{container}"

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

# Example usage
#storage_account = "yourstorageaccount"
#container = "yourcontainer"
#subfolder = "folder1/folder2"
#print(get_blob_subfolder_path(storage_account, container, subfolder))

# if __name__ == "__main__":
#     import sys
    
#     def run_test(test_func, name):
#         """Helper function to run and report test results"""
#         try:
#             test_func()
#             print(f"✓ {name} passed")
#             return True
#         except AssertionError as e:
#             print(f"✗ {name} failed: {e}")
#             return False
    
#     def test_get_blob_container_path():
#         """Test the get_blob_container_path function"""
#         # Test case 1: Basic functionality
#         result = get_blob_container_path("teststorage", "testcontainer")
#         expected = "https://teststorage.blob.core.windows.net/testcontainer"
#         assert result == expected, f"Expected '{expected}', got '{result}'"
        
#         # Test case 2: Special characters in storage account name
#         result = get_blob_container_path("test-storage", "testcontainer")
#         expected = "https://test-storage.blob.core.windows.net/testcontainer"
#         assert result == expected, f"Expected '{expected}', got '{result}'"
        
#         # Test case 3: Special characters in container name
#         result = get_blob_container_path("teststorage", "test-container")
#         expected = "https://teststorage.blob.core.windows.net/test-container"
#         assert result == expected, f"Expected '{expected}', got '{result}'"
    
#     def test_get_blob_subfolder_path():
#         """Test the get_blob_subfolder_path function"""
#         # Test case 1: Basic functionality
#         result = get_blob_subfolder_path("teststorage", "testcontainer", "testfolder")
#         expected = "https://teststorage.blob.core.windows.net/testcontainer/testfolder"
#         assert result == expected, f"Expected '{expected}', got '{result}'"
        
#         # Test case 2: Nested subfolder path
#         result = get_blob_subfolder_path("teststorage", "testcontainer", "folder1/folder2")
#         expected = "https://teststorage.blob.core.windows.net/testcontainer/folder1/folder2"
#         assert result == expected, f"Expected '{expected}', got '{result}'"
        
#         # Test case 3: Subfolder with special characters
#         result = get_blob_subfolder_path("teststorage", "testcontainer", "folder-with-dashes")
#         expected = "https://teststorage.blob.core.windows.net/testcontainer/folder-with-dashes"
#         assert result == expected, f"Expected '{expected}', got '{result}'"
        
#         # Test case 4: Empty subfolder path
#         result = get_blob_subfolder_path("teststorage", "testcontainer", "")
#         expected = "https://teststorage.blob.core.windows.net/testcontainer/"
#         assert result == expected, f"Expected '{expected}', got '{result}'"
    
#     # Run all tests
#     print("Running tests for blobcontainer.py module...")
#     tests = [
#         (test_get_blob_container_path, "get_blob_container_path"),
#         (test_get_blob_subfolder_path, "get_blob_subfolder_path")
#     ]
    
#     failed = 0
#     for test_func, name in tests:
#         if not run_test(test_func, name):
#             failed += 1
    
#     # Report test results
#     total = len(tests)
#     passed = total - failed
#     print(f"\nTest results: {passed}/{total} tests passed")
    
#     # Set exit code based on test results
#     sys.exit(1 if failed > 0 else 0)
