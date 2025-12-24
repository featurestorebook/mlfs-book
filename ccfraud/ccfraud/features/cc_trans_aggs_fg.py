import hopsworks
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import math
import geoip2.database
from datetime import datetime


def prev_ts_transaction(df: pd.DataFrame) -> pd.DataFrame:
    """
    df must have a 'card_id' and 'transaction_time' columns.
    """
    window_spec = Window.partitionBy("card_id").orderBy("transaction_time")

    df = df.withColumn("prev_transaction_time", F.lag(F.col("transaction_time"), 1).over(window_spec))

    return df

def prev_ip_transaction(df: pd.DataFrame) -> pd.DataFrame:
    """
    df must have a 'card_id' and 'transaction_time' columns.
    """
    window_spec = Window.partitionBy("card_id").orderBy("transaction_time")

    df = df.withColumn("prev_transaction_ip", F.lag(F.col("ip_addr"), 1).over(window_spec))

    return df


# if __name__ == "__main__":
#     # Example transactions
#     ip1 = "203.0.113.1"    # New York (example IP)
#     time1 = "2023-03-15T10:30:00"

#     ip2 = "198.51.100.42"  # Tokyo (example IP)
#     time2 = "2023-03-15T11:30:00"  # 1 hour later

#     distance, time_diff, speed = haversine_distance_transactions(ip1, time1, ip2, time2)

#     print(f"Distance between transactions: {distance:.2f} km")
#     print(f"Time between transactions: {time_diff:.2f} hours")
#     print(f"Implied travel speed: {speed:.2f} km/h")

#     if is_impossible_travel(distance, time_diff):
#         print("FRAUD ALERT: Impossible travel detected!")
#     else:
#         print("Travel pattern is physically possible.")
