from pandas import DataFrame
import re
import unicodedata
import string

def clean_whitespace_in_df(df: DataFrame) -> DataFrame:
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

def remove_empty_rows(df: DataFrame) -> DataFrame:
    """
    Remove rows where all values are empty or NaN.
    
    Args:
        df (DataFrame): Input DataFrame
        
    Returns:
        DataFrame: DataFrame with empty rows removed
    """
    return df.dropna(how='all')

def convert_to_numeric(df: DataFrame, columns: list) -> DataFrame:
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

def check_column_completeness(df: DataFrame) -> dict:
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
