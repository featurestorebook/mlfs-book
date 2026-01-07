import pandas as pd
import numpy as np
import time

# Generate sample data
#num_rows = 10_000_000
num_rows = 1
num_rows = 250

# Python function (simulating a Python UDF)
def python_udf_func(x):
    return x * x

# Pandas function (simulating a Pandas UDF)
def pandas_udf_func(x: pd.Series) -> pd.Series:
    return x * x

# Benchmark Python UDF
start_time = time.time()
python_udf_func(4)
python_udf_duration = time.time() - start_time
print(f"Python UDF execution time: {python_udf_duration:.7f} seconds")

# Benchmark Pandas UDF
start_time = time.time()
data = pd.DataFrame({"value": np.arange(num_rows, dtype=float)})
data["pandas_udf_result"] = pandas_udf_func(data["value"])
pandas_udf_duration = time.time() - start_time
print(f"Pandas UDF execution time: {pandas_udf_duration:.7f} seconds")

# Summary
print("\nBenchmark Summary:")
print(f"Python UDF latency: {python_udf_duration:.7f} seconds")
print(f"Pandas UDF latency: {pandas_udf_duration:.7f} seconds")

