import polars as pl

# Read the PSV file in streaming mode
df = pl.scan_csv("VIC_ADDRESS_DETAIL.psv", separator="|")

# Save the DataFrame as a Parquet file using streaming
df.sink_parquet("address_detail.parquet")

print("PSV file successfully converted to Parquet using streaming!")