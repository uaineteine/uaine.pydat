def read_sas_chunk(filepath: str, offset: int, chunksize: int, use_cols: list = None) -> pd.DataFrame:
    """
    Reads a chunk of rows from SAS7BDAT file. Helper function to fetch_sas_parallel.

    Args:
        filepath (str): The filepath to the SAS dataset (.sas7bdat) file that is being converted to a Polars DataFrame
        offset (int): The starting row for the read-in chunk relative to the data being read in. This is used to avoid overlapping of chunks,
        chunksize (int): The number of rows to process at one time, per chunk. 

    Returns:
        pl.DataFrame: A Polars DataFrame representing the read chunk.
    """

    #if usecols is given, do a column check
    if use_cols is not None:
        columns_in_frame = read_sas_colnames(filepath, encoding=encoding)
        if not all(col in columns_in_frame for col in use_cols):
            raise ValueError(f"Column {col} not found in SAS file")

    pandas_df, _ = pyreadstat.read_sas7bdat(
        filename_path=filepath,
        row_offset=offset,
        row_limit=chunksize,
        usecols=use_cols
    )

    return pandas_df

#testing read of dataset.sas7bdat
#code here:

def main():
    """
    Simple tests for read_sas_chunk. Update the 'test_filepath' to point to a valid .sas7bdat file for testing.
    """
    import pandas as pd
    test_filepath = "test_chunk_for_sas/dataset.sas7bdat"  # TODO: Update this path
    offset = 0
    chunksize = 10

    # Test 1: All columns
    use_cols = None
    try:
        df = read_sas_chunk(test_filepath, offset, chunksize, use_cols)
        print("Test 1 (all columns):")
        print(df)
        assert isinstance(df, pd.DataFrame), "Output is not a pandas DataFrame"
        assert len(df) <= chunksize, "Returned chunk is larger than requested chunksize"
        print("Test 1 passed.")
    except Exception as e:
        print(f"Test 1 failed: {e}")

    # Test 2: Only 'var2' column
    use_cols = ['var2']
    try:
        df2 = read_sas_chunk(test_filepath, offset, chunksize, use_cols)
        print("Test 2 (only 'var2' column):")
        print(df2)
        assert isinstance(df2, pd.DataFrame), "Output is not a pandas DataFrame"
        assert len(df2) <= chunksize, "Returned chunk is larger than requested chunksize"
        assert list(df2.columns) == ['var2'], "Returned columns do not match ['var2']"
        print("Test 2 passed.")
    except Exception as e:
        print(f"Test 2 failed: {e}")

if __name__ == "__main__":
    main()
