import polars as pl
import numpy as np
import random

# Parameters
num_rows = 20_0000
names = ["Alice", "Bob"]

# Generate data
ids = np.arange(1, num_rows + 1, dtype=np.int64)
random_names = [random.choice(names) for _ in range(num_rows)]

# Create DataFrame
df = pl.DataFrame({
    "id": ids,
    "name": random_names
})

# Write to Parquet
output_path = "data.parquet"
df.write_parquet(output_path)

print(f"Wrote {num_rows} rows to {output_path}")
