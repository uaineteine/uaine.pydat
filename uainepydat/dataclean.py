def clean_whitespace_in_df(df: DataFrame) -> DataFrame:
    df_cleaned = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    return df_cleaned
