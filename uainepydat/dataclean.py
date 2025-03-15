from pandas import DataFrame
import re

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

#Execution test
# if __name__ == "__main__":
#     print(keep_only_letters("Hello, World!"))
#     print(keep_only_letters("12345"))
#     print(keep_only_letters("Hello, World! 12345"))
#     print(keep_only_letters("Hello, World! 12345"))
#     print(keep_only_letters("C:/")) 
