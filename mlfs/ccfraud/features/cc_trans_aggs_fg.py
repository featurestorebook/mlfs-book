import hopsworks
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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