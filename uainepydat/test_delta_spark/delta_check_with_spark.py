from pyspark.sql import SparkSession
import contextlib
import io
import re

class DeltaCheck:
    def __init__(self, df_before, df_after):
        self.df_before = df_before
        self.df_after = df_after
        self.n_before = df_before.count()
        self.n_after = df_after.count()
        self.delta_rows = df_after.subtract(df_before)
        self.dropped_rows = self.n_after - self.delta_rows.count()
        self.size_before = self.estimate_size_of_df(df_after)
        self.size_after = self.estimate_size_of_df(self.delta_rows)

    @staticmethod
    def estimate_size_of_df(df):
        """Estimate the size in bytes of a PySpark DataFrame using the logical plan."""
        with contextlib.redirect_stdout(io.StringIO()) as stdout:
            df.explain(mode="cost")
        plan = stdout.getvalue().split("\n")
        # Find the first line with sizeInBytes
        for line in plan:
            if "sizeInBytes" in line:
                pattern = r"sizeInBytes=([0-9]+\.?[0-9]*)\s?(B|KiB|MiB|GiB|TiB|EiB)?"
                m = re.search(pattern, line)
                if m:
                    size = float(m.group(1))
                    unit = m.group(2)
                    if unit == "KiB":
                        size *= 1024
                    elif unit == "MiB":
                        size *= 1024 * 1024
                    elif unit == "GiB":
                        size *= 1024 * 1024 * 1024
                    elif unit == "TiB":
                        size *= 1024 * 1024 * 1024 * 1024
                    elif unit == "EiB":
                        size = -1  # Spark returns max value if unknown
                    return size
        return -1

    def summary(self):
        print(f"DeltaCheck:")
        print(f"  Rows before: {self.n_before}")
        print(f"  Rows after: {self.n_after}")
        print(f"  Dropped rows: {self.dropped_rows}")
        print(f"  Estimated size before: {self.size_before} bytes")
        print(f"  Estimated size after: {self.size_after} bytes")
        self.delta_rows.show(truncate=False)

# Initialize Spark session
spark = SparkSession.builder.appName("DeltaCheck").getOrCreate()

df1 = spark.read.parquet("data.parquet")
df2 = spark.read.parquet("data2.parquet")

delta = DeltaCheck(df1, df2)
delta.summary()

spark.stop()
