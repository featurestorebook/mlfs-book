import hopsworks 
import hsfs.feature_group
import pandas as pd
from datetime import timedelta
import json
from hsfs.core import kafka_engine
import hsfs
import numpy as np


def fraud_rate_by_num_days(col: str, df: pd.DataFrame, days: int) -> pd.DataFrame:
    """
    Computes the fraud_rate for a Pandas DataFrame with 'transaction_time' as event_time,
    using a rolling window to calculate fraud transactions within a specified number of days.
    """
    # Convert transaction_time to datetime if it's not already
    df['transaction_time'] = pd.to_datetime(df['transaction_time'])

    # Add transaction_date as a separate column
    df['transaction_date'] = df['transaction_time'].dt.date

    # Sort the DataFrame by the grouping column and transaction_time for rolling
    df = df.sort_values(by=[col, 'transaction_time'])

    # Compute the rolling sum for fraud transactions within the specified window (days)
    df['fraud_transactions'] = (
        df.groupby(col)['is_fraud']  # Group by the specified column
        .rolling(window=f"{days}D", on='transaction_time', min_periods=1)
        .sum()
        .reset_index(level=0, drop=True)  # Drop the group level index added by rolling
    )

    # Compute the rolling count for total transactions in the same window
    df['total_transactions'] = (
        df.groupby(col)['transaction_time']
        .rolling(window=f"{days}D", on='transaction_time', min_periods=1)
        .count()
        .reset_index(level=0, drop=True)
    )

    # Calculate fraud_rate
    df['fraud_rate'] = df['fraud_transactions'] / df['total_transactions']

    # Drop intermediate columns if needed
    return df.drop(columns=['fraud_transactions', 'total_transactions'])


def avg_fraud_rate_last_N_days(df: pd.DataFrame, days: int) -> pd.DataFrame:
    """
    Computes the average fraud rate for the last N days for each merchant, grouped by transaction date.
    """
    # Define column names based on the number of days
    fraud_rate_col = f"merchant_id_fraud_rate_last_{days}_days"
    total_transactions_col = f"merchant_id_total_transactions_{days}d"
    avg_fraud_rate_col = f"merchant_id_avg_fraud_rate_last_{days}_days"

    # Add a transaction_date column if not already present
    if "transaction_date" not in df.columns:
        df['transaction_date'] = pd.to_datetime(df['transaction_time']).dt.date

        

    # Calculate weighted fraud rate directly with vectorized operations
    df['weighted_fraud_sum'] = df[fraud_rate_col] * df[total_transactions_col]

    # Aggregate using groupby
    aggregated = df.groupby(['transaction_date', 'merchant_id']).agg(
        weighted_fraud_sum=('weighted_fraud_sum', 'sum'),
        total_transactions_sum=(total_transactions_col, 'sum')
    ).reset_index()

    # Compute the average fraud rate
    aggregated[avg_fraud_rate_col] = aggregated['weighted_fraud_sum'] / aggregated['total_transactions_sum']

    # Drop intermediate columns if needed
    return aggregated[['transaction_date', 'merchant_id', avg_fraud_rate_col]]

def get_kafka_config(fs: hsfs.feature_store.FeatureStore):
    # kafka_config = fs._storage_connector_api.get_kafka_connector(fs.id, True).confluent_options()
    kafka_config = kafka_engine.get_kafka_config(fs.id, {})
    ca = kafka_config.pop('ssl.ca.location')
    certificate = kafka_config.pop('ssl.certificate.location')
    key = kafka_config.pop('ssl.key.location')
    kafka_config["ssl.ca.pem"] = open(ca, 'rt').read()
    kafka_config["ssl.certificate.pem"] = open(certificate, 'rt').read()
    kafka_config["ssl.key.pem"] = open(key, 'rt').read()
    return kafka_config


def read_fg_connector(fs: hsfs.feature_store.FeatureStore) -> str:
    return json.dumps({
        "transport": {
            "name": "kafka_input",
            "config": kafka_config | {"topics": [topic_name], "auto.offset.reset": "earliest"}
        },
        "format": {
            "name": "json",
            "config": {
                "update_format": "raw",
                "array": False
            }
        }
    })

def read_card_details_connector(fg: hsfs.feature_store.FeatureGroup) -> str:
    return json.dumps({
        "transport": {
            "name": "kafka_input",
            "config": kafka_config | {"topics": [topic_name], "auto.offset.reset": "earliest"}
        },
        "format": {
            "name": "json",
            "config": {
                "update_format": "raw",
                "array": False
            }
        }
    })


def read_stream_connector(fs: hsfs.feature_store.FeatureStore, topic_name: str) -> str:
    kafka_config = get_kafka_config(fs)
    return json.dumps({
        "transport": {
            "name": "kafka_input",
            "config": kafka_config | {"topics": [topic_name], "auto.offset.reset": "earliest"}
        },
        "format": {
            "name": "json",
            "config": {
                "update_format": "raw",
                "array": False
            }
        }
    })


def insert_stream_connector(project: hopsworks.project, fs: hsfs.feature_store.FeatureStore, 
                            fg: hsfs.feature_group) -> str :
    kafka_config = get_kafka_config(fs)
    config = kafka_config | {
        "topic": fg._online_topic_name,
        "auto.offset.reset": "earliest",
        "headers": [
            {
                'key': 'projectId',
                'value': str(project.id),
            },
            {
                'key': 'featureGroupId',
                'value': str(fg.id),
            },
            {
                'key': 'subjectId',
                'value': str(fg.subject["id"]),
            },
        ]
    }

    return json.dumps({
        "transport": {
            "name": "kafka_output",
            "config": config
        },
        "format": {
            "name": "avro",
            "config": {
                "schema": fg.avro_schema,
                "skip_schema_id": True
            }
        }
    })
    
# from pyspark.sql import functions as F
# from pyspark.sql.window import Window
# def fraud_rate_by_num_days(col: str, df: pd.DataFrame, days: int) -> pd.DataFrame:
#     """
#     Computes the fraud_rate for a df with 'transaction_time' as partition key,
#     and 'transaction_date' as event_time. 
#     """
#     # There are 8640 seconds in one day
#     day = 86400

#     # Define a window that looks at the last 30 days for each merchant/location, sliding 1 day at a time
#     window_spec = Window.partitionBy(f"{col}").orderBy(F.col("transaction_time").cast("long")).rangeBetween(-days * day, 0)

#     # Compute total transactions and fraud transactions within the number of days window
#     df = df.withColumn(f"{col}_total_transactions_{days}d", F.count("transaction_id").over(window_spec)) \
#            .withColumn(f"fraud_transactions_{days}d", F.sum("is_fraud").over(window_spec)) \
#            .withColumn(f"{col}_fraud_rate_last_{days}_days", F.col(f"fraud_transactions_{days}d") / F.col(f"total_transactions_{days}d")) \
    
#     # reduce the output to one row per date - we will provide only a single fraud rate for 
#     # each day for each  merchant/location
#     df = df.withColumn("transaction_date", F.to_date(F.col("transaction_time")))
#     df = df.groupBy("transaction_date", f"{col}") \
#         .agg(
#             F.avg(f"{col}_fraud_rate_last_{days}_days")
#            .withColumn(f"{col}_fraud_rate_last_{days}_days", F.col(f"fraud_transactions_{days}d") / F.col(f"{col}_total_transactions_{days}d")) \
#             .alias("{col}_avg_fraud_rate_last_{days}_days"),
#         )
#     return df


# def fraud_rate_by_num_days(col: str, df: pl.DataFrame, days: int) -> pl.DataFrame:
#     """
#     Computes the fraud_rate for a Polars DataFrame with 'transaction_time' as event_time,
#     and 'transaction_date' as event_time.
#     """
#     # Convert days to seconds
#     day_seconds = days * 86400

#     # Add transaction_date as a separate column
#     df = df.with_columns(
#         (pl.col("transaction_time").cast(pl.Date)).alias("transaction_date")
#     )

#     # Create a rolling window aggregation to calculate total and fraud transactions
#     df = df.with_columns([
#         pl.col("transaction_time")
#         .cast(pl.Datetime)
#         .over(pl.col(col))
#         .rolling_sum(
#             "transaction_time",
#             window=f"{day_seconds}s",
#             by=col,
#             # weights=[...]
#         )
#     ])
#     return df.drop(f"fraud_transactions_{days}d")

    # def avg_fraud_rate_last_N_days(df: pl.DataFrame, days: int) -> pl.DataFrame:
#     """
#     Computes the average fraud rate for the last N days for each merchant, grouped by transaction date.
#     """
#     # Define column names based on the number of days
#     fraud_rate_col = f"merchant_id_fraud_rate_last_{days}_days"
#     total_transactions_col = f"merchant_id_total_transactions_{days}d"
#     avg_fraud_rate_col = f"merchant_id_avg_fraud_rate_last_{days}_days"

#     # Add a transaction_date column if not already present
#     if "transaction_date" not in df.columns:
#         df = df.with_columns(
#             (pl.col("transaction_time").cast(pl.Date)).alias("transaction_date")
#         )

#     # Group by transaction_date and merchant_id, and compute the weighted average fraud rate
#     df = df.groupby(["transaction_date", "merchant_id"]).agg([
#         (pl.col(fraud_rate_col) * pl.col(total_transactions_col)).sum() / pl.col(total_transactions_col).sum()
#         .alias(avg_fraud_rate_col)
#     ])

#     return df

# def avg_fraud_rate_last_N_days(df: pd.DataFrame, days: int) -> pd.DataFrame:
#     """
#     """
#     # by grouping by transaction_date, we reduce the number of rows to one per day
#     df = df.groupBy("transaction_date", f"merchant_id") \
#         .agg(
#             # TODO - compute the weighted average  using total_transactions_{days}d
#             # F.avg(f"{col}_fraud_rate_last_{days}_days")
#             F.weighted_avg(f"merchant_id_fraud_rate_last_{days}_days", f"merchant_id_total_transactions_{days}d")
#            .withColumn(f"merchant_id_fraud_rate_last_{days}_days", F.col(f"fraud_transactions_{days}d") / F.col(f"{col}_total_transactions_{days}d")) \

#             .alias("{col}_avg_fraud_rate_last_{days}_days"),
#         )
#     return df
