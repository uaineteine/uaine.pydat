from pandas import DataFrame

def clean_whitespace_in_df(df: DataFrame) -> DataFrame:
    """
    Remove leading and trailing whitespace from string columns in a DataFrame.

    Parameters:
    df (DataFrame): The input DataFrame.

    Returns:
    DataFrame: The DataFrame with leading and trailing whitespace removed from string columns.
    """
    df_cleaned = df.apply(p() if x.dtype == "object" else x)
    return df_cleaned
