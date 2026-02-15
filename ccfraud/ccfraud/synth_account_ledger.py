#!/usr/bin/env python3
"""
NDI (Net Disposable Income) Stability - Synthetic Account Ledger Generator

Generates synthetic credit/debit ledger data for NDI feature computation.
Each row represents a credit (payment) or debit (charge) on an account.

Usage:
    python ccfraud/synth_account_ledger.py
    python ccfraud/synth_account_ledger.py --num-accounts 2 --rows-per-account 1000
    python ccfraud/synth_account_ledger.py --start-date 2025-01-01 --end-date 2026-01-01
"""

import sys
from pathlib import Path
import argparse
from datetime import datetime, timedelta

import numpy as np
import polars as pl
import hopsworks
from hsfs.feature import Feature

# Setup paths
current_file = Path(__file__).absolute()
ccfraud_pkg_dir = current_file.parent        # ccfraud/ccfraud/
ccfraud_project_dir = ccfraud_pkg_dir.parent  # ccfraud/
root_dir = ccfraud_project_dir.parent         # mlfs-book/

sys.path.insert(0, str(root_dir))
sys.path.insert(0, str(ccfraud_project_dir))

from mlfs import config


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate synthetic credit/debit ledger data for NDI features"
    )
    parser.add_argument("--num-accounts", type=int, default=10,
                        help="Number of accounts (default: 10)")
    parser.add_argument("--rows-per-account", type=int, default=1_000_000,
                        help="Rows per account (default: 1000000)")
    parser.add_argument("--start-date", type=str, default=None,
                        help="Start date YYYY-MM-DD (default: 12 months ago)")
    parser.add_argument("--end-date", type=str, default=None,
                        help="End date YYYY-MM-DD (default: today)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed (default: 42)")
    parser.add_argument("--env-file", type=str, default=None,
                        help="Path to .env file (default: <root>/.env)")
    return parser.parse_args()


def generate_account_ledger(
    num_accounts: int,
    rows_per_account: int,
    start_date: datetime,
    end_date: datetime,
    seed: int = 42,
) -> pl.DataFrame:
    """Generate synthetic credit/debit ledger data.

    - ~85% debits (purchases/charges): log-normal(mu=3.5, sigma=1.2), clamped $1-$5000
    - ~15% credits (payments): log-normal(mu=6.5, sigma=0.8), clamped $100-$10000
    - Timestamps uniformly distributed over the date range
    """
    rng = np.random.default_rng(seed)
    total_rows = num_accounts * rows_per_account
    total_seconds = max(int((end_date - start_date).total_seconds()), 1)

    print(f"Generating {total_rows:,} ledger rows for {num_accounts} accounts...")

    # Account IDs: repeat each account_id rows_per_account times
    account_ids = np.repeat(
        [f"ACC_{i:08d}" for i in range(num_accounts)],
        rows_per_account,
    )

    # Transaction IDs
    transaction_ids = np.arange(total_rows, dtype=np.int64)

    # Determine which rows are credits vs debits
    is_credit = rng.random(total_rows) < 0.15  # ~15% credits

    # Generate amounts
    credit_amounts = np.clip(
        rng.lognormal(mean=6.5, sigma=0.8, size=total_rows), 100.0, 10_000.0
    )
    debit_amounts = np.clip(
        rng.lognormal(mean=3.5, sigma=1.2, size=total_rows), 1.0, 5_000.0
    )

    credit_col = np.where(is_credit, np.round(credit_amounts, 2), 0.0)
    debit_col = np.where(~is_credit, np.round(debit_amounts, 2), 0.0)

    # Timestamps: uniform random offsets from start_date
    offsets = rng.integers(0, total_seconds, size=total_rows)
    event_times = [start_date + timedelta(seconds=int(s)) for s in offsets]

    df = pl.DataFrame({
        "transaction_id": transaction_ids,
        "account_id": account_ids,
        "credit": credit_col,
        "debit": debit_col,
        "event_time": event_times,
    })

    print(f"  Credits: {is_credit.sum():,} rows  |  Debits: {(~is_credit).sum():,} rows")
    return df


def main():
    args = parse_args()

    # Load settings
    env_file = args.env_file or str(root_dir / ".env")
    print(f"Loading environment from: {env_file}")
    settings = config.HopsworksSettings(_env_file=env_file)

    # Date range
    if args.end_date:
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").replace(
            hour=23, minute=59, second=59
        )
    else:
        end_date = datetime.now().replace(hour=23, minute=59, second=59)

    if args.start_date:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    else:
        start_date = end_date - timedelta(days=365)

    print(f"Date range: {start_date.date()} to {end_date.date()}")

    # Generate data
    df = generate_account_ledger(
        num_accounts=args.num_accounts,
        rows_per_account=args.rows_per_account,
        start_date=start_date,
        end_date=end_date,
        seed=args.seed,
    )

    # Connect to Hopsworks
    print("\nConnecting to Hopsworks...")
    project = hopsworks.login()
    fs = project.get_feature_store()

    # Create or get the feature group
    fg = fs.get_or_create_feature_group(
        name="account_ledger",
        version=1,
        description="Credit/debit ledger per account for NDI stability features",
        primary_key=["transaction_id"],
        event_time="event_time",
        online_enabled=True,
        stream=True,
        features=[
            Feature("transaction_id", type="bigint"),
            Feature("account_id", type="string"),
            Feature("credit", type="double"),
            Feature("debit", type="double"),
            Feature("event_time", type="timestamp"),
        ],
    )

    try:
        fg.save()
    except Exception:
        print("Feature group already exists")

    # Insert data
    print(f"\nInserting {len(df):,} rows into account_ledger feature group...")
    fg.insert(df)

    print("\nDone! account_ledger feature group populated.")


if __name__ == "__main__":
    main()
