#!/usr/bin/env python
"""Batch feature pipeline for credit card transactions (Polars).

Reads the credit card transactions and the fraud labels, computes per-transaction
features (time since the card's previous transaction, the fraud label) and inserts them
into the cc_trans_fg feature group, whose on-demand transformation adds the
impossible-travel feature (haversine_distance). Runs as a Hopsworks Python job.
"""

import sys
from pathlib import Path
import warnings
import argparse
import hopsworks
import polars as pl
from datetime import datetime
import hsfs

current_file = Path(__file__).absolute()
ccfraud_pkg_dir = current_file.parent  # ccfraud/ccfraud/
ccfraud_project_dir = ccfraud_pkg_dir.parent  # ccfraud/
root_dir = ccfraud_project_dir.parent  # mlfs-book/
root_dir = str(root_dir)

sys.path.insert(0, str(root_dir))
sys.path.insert(0, str(ccfraud_project_dir))

# Set the environment variables from the .env file (outside Hopsworks; a job has no .env)
if Path(f"{root_dir}/.env").exists():
    from mlfs import config
    settings = config.HopsworksSettings(_env_file=f"{root_dir}/.env")

from ccfraud.features import cc_trans_fg
from ccfraud.features.common import ensure_statistics_disabled
cc_trans_fg.root_dir = str(root_dir)


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Batch feature pipeline for credit card transactions"
    )
    parser.add_argument(
        "--last-processed-date",
        type=str,
        default="2025-01-01",
        help="Last processed date in YYYY-MM-DD format (default: 2025-01-01)"
    )
    parser.add_argument(
        "--current-date",
        type=str,
        default="2025-10-05",
        help="Current date in YYYY-MM-DD format (default: 2025-10-05)"
    )
    parser.add_argument(
        "--env-file",
        type=str,
        default=None,
        help="Path to .env file (default: <root_dir>/.env)"
    )
    parser.add_argument(
        "--wait",
        action="store_true",
        default=False,
        help="Wait for data to be synced to backend (default: False)"
    )
    return parser.parse_args()


def transaction_features(trans_df: pl.DataFrame, fraud_df: pl.DataFrame) -> pl.DataFrame:
    """Per-transaction features: the previous transaction of the same card, the time since it,
    and the fraud label. One row per (cc_num, ts)."""
    trans_df = trans_df.sort(["cc_num", "ts"]).with_columns(
        pl.col("ts").shift(1).over("cc_num").alias("prev_ts"),
        pl.col("card_present").shift(1).over("cc_num").alias("prev_card_present"),
        pl.col("ip_address").shift(1).over("cc_num").alias("prev_ip_address"),
        pl.col("t_id").is_in(fraud_df["t_id"].to_list()).alias("is_fraud"),
    )
    trans_df = trans_df.with_columns(
        time_since_last_trans=cc_trans_fg.time_since_last_trans(trans_df["ts"], trans_df["prev_ts"]),
        days_to_card_expiry=pl.lit(0, dtype=pl.Int64),  # placeholder for now
        # A card's first transaction has no previous one. The haversine_distance UDF (pandas)
        # receives these columns as Arrow-backed pandas series, where a null is pd.NA and cannot
        # be tested as a boolean, so give it the values it treats as "no previous transaction".
        prev_ip_address=pl.col("prev_ip_address").fill_null(""),
        prev_card_present=pl.col("prev_card_present").fill_null(False),
    ).drop("prev_ts")
    # Primary key cc_num + event time ts: keep the last of any duplicates
    return trans_df.unique(subset=["cc_num", "ts"], keep="last", maintain_order=True)


def main(last_processed_date, current_date, wait=False):
    """Main execution function for the batch feature pipeline."""

    # Connect to Hopsworks
    print("Connecting to Hopsworks...")
    project = hopsworks.login()
    fs = project.get_feature_store()

    # Get existing feature groups
    print("Getting feature groups...")
    trans_fg = fs.get_feature_group("credit_card_transactions", version=1)
    cc_fraud_fg = fs.get_feature_group("cc_fraud", version=1)

    # Get or create the cc_trans_fg feature group
    name = "cc_trans_fg"
    cc_trans_fg_group = fs.get_or_create_feature_group(
        name=name,
        primary_key=["cc_num"],
        online_enabled=True,
        version=1,
        event_time="ts",
        features=[
            hsfs.feature.Feature("t_id", type="bigint"),
            hsfs.feature.Feature("cc_num", type="string"),
            hsfs.feature.Feature("merchant_id", type="string"),
            hsfs.feature.Feature("account_id", type="string"),
            hsfs.feature.Feature("amount", type="double"),
            hsfs.feature.Feature("ip_address", type="string"),
            hsfs.feature.Feature("card_present", type="boolean"),
            hsfs.feature.Feature("time_since_last_trans", type="bigint"),
            hsfs.feature.Feature("days_to_card_expiry", type="bigint"),
            hsfs.feature.Feature("is_fraud", type="boolean"),
            hsfs.feature.Feature("haversine_distance", type="boolean"),
            hsfs.feature.Feature("ts", type="timestamp"),
        ],
        transformation_functions=[cc_trans_fg.haversine_distance],
        parents=[trans_fg],
        # a Python insert into a feature group with statistics enabled launches a PySpark job
        statistics_config=False,
    )

    # Save the feature group if it doesn't exist
    try:
        cc_trans_fg_group.save()
        print("Feature Group created successfully")
    except Exception as e:
        print("Feature Group already exists")
    ensure_statistics_disabled(cc_trans_fg_group)

    # Read transaction data filtered by last processed date
    print(f"Reading transactions after {last_processed_date}...")
    #trans_df = trans_fg.filter(hsfs.feature.Feature("ts") > last_processed_date).read(dataframe_type="polars")
    trans_df = trans_fg.read(dataframe_type="polars")
    print(f"Read {trans_df.height} transactions")

    # Read fraud data
    print("Reading fraud data...")
    fraud_df = cc_fraud_fg.select(["t_id"]).read(dataframe_type="polars")
    print(f"Read {fraud_df.height} fraud records")

    print("Computing per-card lag features, time since last transaction and the fraud label...")
    before_dedup = trans_df.height
    trans_df = transaction_features(trans_df, fraud_df)
    print(f"Fraud count: {trans_df['is_fraud'].sum()}")
    print(f"Removed {before_dedup - trans_df.height} duplicate records")

    # Insert into feature store (this will also apply on-demand transformations)
    print("Inserting data into feature store...")
    cc_trans_fg_group.insert(trans_df, wait=wait)

    print("Batch feature pipeline completed successfully!")


if __name__ == "__main__":
    args = parse_args()

    # Parse dates from string arguments
    last_processed_date = datetime.strptime(args.last_processed_date, "%Y-%m-%d")
    current_date = datetime.strptime(args.current_date, "%Y-%m-%d")

    print(f"Processing transactions from {last_processed_date} to {current_date}")

    main(last_processed_date, current_date, wait=args.wait)
