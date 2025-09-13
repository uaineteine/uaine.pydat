import polars as pl

# Define chunk size
chunk_size = 100_000  
filename = "address_detail.parquet"

# Read the data lazily
df_lazy = pl.scan_parquet(filename)
# Get total row count lazily
total_rows = df_lazy.select(pl.count()).collect().item()
print(total_rows)

# Deduplicate each chunk and append it to the final dataset
final_df = pl.DataFrame()

for i in range(0, total_rows, chunk_size):  # Example assuming 10M rows
    chunk = df_lazy.slice(i, chunk_size).unique()  # Deduplicate each chunk lazily
    chunk = chunk.collect(engine="streaming")  # Collect when necessary
    final_df = pl.concat([final_df, chunk], how="vertical")  # Append chunk

# Perform final deduplication after appending all chunks
final_df = final_df.unique()

print(final_df)

print("Deduplication complete. Final dataset saved.")
