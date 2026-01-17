#!/usr/bin/env python
"""Batch feature pipeline for credit card transactions.

This script processes credit card transactions and fraud data, calculates
features like time since last transaction, and inserts the data into the
feature store.
"""

import sys
from pathlib import Path
import warnings
import argparse
import hopsworks
from datetime import datetime
from hsfs.feature import Feature

current_file = Path(__file__).absolute()
ccfraud_pkg_dir = current_file.parent  # ccfraud/ccfraud/
ccfraud_project_dir = ccfraud_pkg_dir.parent  # ccfraud/
root_dir = ccfraud_project_dir.parent  # mlfs-book/
root_dir = str(root_dir) 

sys.path.insert(0, str(root_dir))
sys.path.insert(0, str(ccfraud_project_dir))

# Set the environment variables from the .env file
from mlfs import config
settings = config.HopsworksSettings(_env_file=f"{root_dir}/.env")

from ccfraud.features import cc_trans_fg
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
    return parser.parse_args()


def main(last_processed_date, current_date):
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
        primary_key=["t_id"],
        online_enabled=True,
        version=1,
        event_time="ts",
        features=[
            Feature("t_id", type="bigint"),
            Feature("cc_num", type="string"),
            Feature("merchant_id", type="string"),
            Feature("account_id", type="string"),
            Feature("amount", type="double"),
            Feature("ip_address", type="string"),
            Feature("card_present", type="boolean"),
            Feature("time_since_last_trans", type="bigint"),
            Feature("days_to_card_expiry", type="bigint"),
            Feature("is_fraud", type="boolean"),
            Feature("ts", type="timestamp"),
        ],
        transformation_functions=[cc_trans_fg.haversine_distance],
        parents=[trans_fg]
    )

    # Save the feature group if it doesn't exist
    try:
        cc_trans_fg_group.save()
        print("Feature Group created successfully")
    except Exception as e:
        print("Feature Group already exists")

    # Read transaction data filtered by last processed date
    print(f"Reading transactions after {last_processed_date}...")
    #trans_df = trans_fg.filter(Feature("ts") > last_processed_date).read()
    trans_df = trans_fg.read()
    print(f"Read {len(trans_df)} transactions")

    # Read fraud data
    print("Reading fraud data...")
    fraud_df = cc_fraud_fg.read()
    print(f"Read {len(fraud_df)} fraud records")

    # Sort by cc_num and ts
    print("Sorting transactions by cc_num and ts...")
    trans_df = trans_df.sort_values(["cc_num", "ts"])

    # Create lag features
    print("Creating lag features...")
    trans_df["prev_ts"] = trans_df["ts"].shift(1)
    trans_df["prev_card_present"] = trans_df["card_present"].shift(1)
    trans_df["prev_ip_address"] = trans_df["ip_address"].shift(1)

    # Mark fraudulent transactions
    print("Marking fraudulent transactions...")
    trans_df["is_fraud"] = trans_df["t_id"].isin(fraud_df["t_id"])
    print(f"Fraud count: {trans_df['is_fraud'].sum()}")

    # Calculate time since last transaction
    print("Calculating time since last transaction...")
    from ccfraud import features
    trans_df['time_since_last_trans'] = features.cc_trans_fg.time_since_last_trans(
        trans_df['ts'],
        trans_df['prev_ts']
    )

    # Drop intermediate columns
    trans_df = trans_df.drop(columns=['prev_ts'])

    # Add days_to_card_expiry (placeholder for now)
    trans_df['days_to_card_expiry'] = 0

    # Insert into feature store (this will also apply on-demand transformations)
    print("Inserting data into feature store...")
    cc_trans_fg_group.insert(trans_df)

    print("Batch feature pipeline completed successfully!")


if __name__ == "__main__":
    args = parse_args()

    # Parse dates from string arguments
    last_processed_date = datetime.strptime(args.last_processed_date, "%Y-%m-%d")
    current_date = datetime.strptime(args.current_date, "%Y-%m-%d")

    print(f"Processing transactions from {last_processed_date} to {current_date}")

    main(last_processed_date, current_date)
