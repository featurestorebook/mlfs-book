import hopsworks 
import pandas as pd
import common

def count_chargeback_rate_prev_month(merchant_df: pd.DataFrame) -> pd.DataFrame:
    """
    """
    merchant_df = common.fraud_rate_by_num_days("merchant_id", merchant_df, 7)
    return merchant_df

def count_chargeback_rate_prev_week(merchant_df: pd.DataFrame) -> pd.DataFrame:
    """
    """
    merchant_df = common.fraud_rate_by_num_days("merchant_id", merchant_df, 30)
    return merchant_df

def count_chargeback_rate_prev_day(location: pd.Series) -> pd.Series:
    """
    """
    merchant_df = common.fraud_rate_by_num_days("merchant_id", merchant_df, 1)
    return merchant_df


# weekly_fraud = (
#     transactions_fraud
#     .with_columns(
#         pl.col("ts").dt.week().alias("week"),
#         pl.col("ts").dt.year().alias("year")
#     )
#     .group_by(["merchant_id", "year", "week"])
#     .agg(
#         pl.sum("amount").alias("total_fraud_amount")
#     )
# )

# # Calculate total transactions per merchant per week and month
# weekly_transactions = (
#     transaction_df
#     .with_columns(
#         pl.col("ts").dt.week().alias("week"),
#         pl.col("ts").dt.year().alias("year")
#     )
#     .group_by(["merchant_id", "year", "week"])
#     .agg(
#         pl.sum("amount").alias("total_amount")
#     )
# )

# # Calculate fraud rate by week and month
# weekly_fraud_rate = (
#     weekly_fraud.join(
#         weekly_transactions, on=["merchant_id", "year", "week"], how="inner"
#     )
#     .with_columns(
#         (pl.col("total_fraud_amount") / pl.col("total_amount")).alias("weekly_fraud_rate")
#     )
#     .join(
#         merchant_df, on="merchant_id", how="inner"
#     )
#     .filter(
#         (pl.col("year") == pl.col("last_modified").dt.year()) &
#         (pl.col("week") == pl.col("last_modified").dt.week())
#     )
# )

# merchant_df = merchant_df.join(weekly_fraud_rate.select(["merchant_id", "weekly_fraud_rate"]), on="merchant_id", how="left")

# nan_count = merchant_df["weekly_fraud_rate"].is_nan().sum()

print("NaN count in weekly_fraud_rate:", nan_count)