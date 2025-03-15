import re
import unicodedata
import string
import pandas as pd
import numpy as np

def clean_whitespace_in_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove leading and trailing whitespace from string columns in a DataFrame.

    Parameters:
    df (DataFrame): The input DataFrame.

    Returns:
    DataFrame: The DataFrame with leading and trailing whitespace removed from string columns.
    """
    df_cleaned = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    return df_cleaned

def keep_only_letters(input_string: str) -> str:
    """
    Filter a string to keep only alphabetic characters (letters).
    
    Args:
        input_string (str): The input string to filter
        
    Returns:
        str: String containing only letters from the input
    """
    return re.sub(r'[^a-zA-Z]', '', input_string)

def keep_alphanumeric(input_string: str) -> str:
    """
    Filter a string to keep only alphanumeric characters.
    
    Args:
        input_string (str): The input string to filter
        
    Returns:
        str: String containing only alphanumeric characters
    """
    return re.sub(r'[^a-zA-Z0-9]', '', input_string)

def normalize_text(input_string: str) -> str:
    """
    Normalize text by converting to lowercase and removing accents.
    
    Args:
        input_string (str): The input string to normalize
        
    Returns:
        str: Normalized string
    """
    # Convert to lowercase
    text = input_string.lower()
    # Remove accents
    text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')
    return text

def remove_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows where all values are empty or NaN.
    
    Args:
        df (DataFrame): Input DataFrame
        
    Returns:
        DataFrame: DataFrame with empty rows removed
    """
    return df.dropna(how='all')

def convert_to_numeric(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """
    Convert specified columns to numeric type, with errors coerced to NaN.
    
    Args:
        df (DataFrame): Input DataFrame
        columns (list): List of column names to convert
        
    Returns:
        DataFrame: DataFrame with specified columns converted to numeric
    """
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def check_column_completeness(df: pd.DataFrame) -> dict:
    """
    Calculate the percentage of non-missing values for each column.
    
    Args:
        df (DataFrame): Input DataFrame
        
    Returns:
        dict: Dictionary mapping column names to completeness percentage
    """
    return {col: (1 - df[col].isna().mean()) * 100 for col in df.columns}
#Execution test
# if __name__ == "__main__":
#     print(keep_only_letters("Hello, World!"))
#     print(keep_only_letters("12345"))
#     print(keep_only_letters("Hello, World! 12345"))
#     print(keep_only_letters("Hello, World! 12345"))
#     print(keep_only_letters("C:/")) 
# Test string manipulation functions
    # print("=== String manipulation functions ===")
    # print(f"keep_only_letters: {keep_only_letters('Hello, World!')}")
    # print(f"keep_only_letters: {keep_only_letters('12345')}")
    # print(f"keep_only_letters: {keep_only_letters('Hello, World! 12345')}")
    # print(f"keep_alphanumeric: {keep_alphanumeric('Hello, World! 12345')}")
    # print(f"keep_alphanumeric: {keep_alphanumeric('Special @#$% chars')}")
    # print(f"normalize_text: {normalize_text('Héllò Wörld!')}")
    # print(f"normalize_text: {normalize_text('UPPER case TEXT')}")
    
    # # Create sample DataFrame for testing DataFrame functions
    # print("\n=== DataFrame functions ===")
    # df = pd.DataFrame({
    #     'A': ['  text with spaces  ', 'more  text', '  leading/trailing  '],
    #     'B': [1, 2, np.nan],
    #     'C': [np.nan, np.nan, np.nan],
    #     'D': ['1.5', '2,000', 'not-a-number']
    # })
    
    # print("\nOriginal DataFrame:")
    # print(df)
    
    # # Test clean_whitespace_in_df
    # df_clean = clean_whitespace_in_df(df)
    # print("\nAfter clean_whitespace_in_df:")
    # print(df_clean)
    
    # # Test remove_empty_rows
    # df_with_empty = pd.DataFrame({
    #     'A': [1, np.nan, 3],
    #     'B': [4, np.nan, 6],
    #     'C': [7, np.nan, 9]
    # })
    # df_no_empty = remove_empty_rows(df_with_empty)
    # print("\nBefore remove_empty_rows:")
    # print(df_with_empty)
    # print("\nAfter remove_empty_rows:")
    # print(df_no_empty)
    
    # # Test convert_to_numeric
    # # Fix the function to use pd_to_numeric instead of pd.to_numeric
    # def convert_to_numeric_fixed(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    #     df = df.copy()
    #     for col in columns:
    #         if col in df.columns:
    #             df[col] = pd.to_numeric(df[col], errors='coerce')
    #     return df
    
    # df_numeric = convert_to_numeric_fixed(df, ['B', 'D'])
    # print("\nAfter convert_to_numeric:")
    # print(df_numeric)
    
    # # Test check_column_completeness
    # completeness = check_column_completeness(df)
    # print("\nColumn completeness:")
    # for col, percent in completeness.items():
    #     print(f"{col}: {percent:.1f}%")

