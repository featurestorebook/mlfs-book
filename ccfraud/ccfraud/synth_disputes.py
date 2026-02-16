#!/usr/bin/env python3
"""
Synthetic Disputed Transactions Generator

Reads the credit_card_transactions feature group, randomly samples ~5-10%
of transactions as disputed, and creates a disputed_transactions feature
group with dispute metadata including card entry mode.

Usage:
    python ccfraud/synth_disputes.py
    python ccfraud/synth_disputes.py --dispute-rate 0.1
    python ccfraud/synth_disputes.py --dispute-rate 0.05 --seed 123
"""

import argparse
from datetime import timedelta

import numpy as np
import polars as pl
import hopsworks
from hsfs.feature import Feature


# ---------------------------------------------------------------------------
# CLI arguments
# ---------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate synthetic disputed transaction data"
    )
    parser.add_argument("--dispute-rate", type=float, default=0.08,
                        help="Fraction of transactions to mark as disputed (default: 0.08)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed (default: 42)")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Generate disputes from existing transactions
# ---------------------------------------------------------------------------
def generate_disputes(
    transactions_df: pl.DataFrame,
    dispute_rate: float,
    rng: np.random.Generator,
) -> pl.DataFrame:
    """Sample transactions and create dispute records."""

    n_total = len(transactions_df)
    n_disputes = int(n_total * dispute_rate)

    print(f"  Total transactions: {n_total:,}")
    print(f"  Dispute rate: {dispute_rate:.1%}")
    print(f"  Disputes to generate: {n_disputes:,}")

    # Sample transaction indices
    dispute_indices = rng.choice(n_total, size=n_disputes, replace=False)
    disputed_txns = transactions_df[dispute_indices.tolist()]

    # Build dispute rows
    dispute_ids = list(range(n_disputes))

    # card_entry_mode: 'swiped' if card_present == True, 'keyed' if False
    card_entry_modes = disputed_txns.select(
        pl.when(pl.col("card_present") == True)
        .then(pl.lit("swiped"))
        .otherwise(pl.lit("keyed"))
        .alias("card_entry_mode")
    ).to_series().to_list()

    # event_time: dispute happens 1-72 hours after the transaction
    offsets_hours = rng.uniform(1, 72, size=n_disputes)
    ts_values = disputed_txns["ts"].to_list()
    event_times = [
        ts + timedelta(hours=float(offset))
        for ts, offset in zip(ts_values, offsets_hours)
    ]

    df = pl.DataFrame(
        {
            "dispute_id": dispute_ids,
            "t_id": disputed_txns["t_id"].to_list(),
            "merchant_id": disputed_txns["merchant_id"].to_list(),
            "card_entry_mode": card_entry_modes,
            "event_time": event_times,
        },
        schema={
            "dispute_id": pl.Int64,
            "t_id": pl.Int64,
            "merchant_id": pl.Utf8,
            "card_entry_mode": pl.Utf8,
            "event_time": pl.Datetime,
        },
    )

    print(f"  disputed_transactions: {len(df):,} rows")
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    args = parse_args()
    rng = np.random.default_rng(args.seed)

    # Connect to Hopsworks and read existing transactions
    print("Connecting to Hopsworks...")
    project = hopsworks.login()
    fs = project.get_feature_store()

    print("\nReading credit_card_transactions feature group...")
    txn_fg = fs.get_feature_group("credit_card_transactions", version=1)
    transactions_df = pl.from_pandas(txn_fg.read())
    print(f"  Loaded {len(transactions_df):,} transactions")

    # Generate disputes
    print("\nGenerating disputed transactions...")
    disputes_df = generate_disputes(
        transactions_df=transactions_df,
        dispute_rate=args.dispute_rate,
        rng=rng,
    )

    # Create/insert into disputed_transactions feature group
    print("\n--- disputed_transactions ---")
    fg = fs.get_or_create_feature_group(
        name="disputed_transactions",
        version=1,
        description="Disputed credit card transactions with card entry mode",
        primary_key=["dispute_id"],
        event_time="event_time",
        online_enabled=False,
        features=[
            Feature("dispute_id", type="bigint"),
            Feature("t_id", type="bigint"),
            Feature("merchant_id", type="string"),
            Feature("card_entry_mode", type="string"),
            Feature("event_time", type="timestamp"),
        ],
    )
    try:
        fg.save()
    except Exception:
        print("  Feature group disputed_transactions already exists")

    print(f"  Inserting {len(disputes_df):,} rows into disputed_transactions...")
    fg.insert(disputes_df)

    print("\nDone! Feature group populated: disputed_transactions.")


if __name__ == "__main__":
    main()
