#!/usr/bin/env python3
"""
Synthetic Money Transfer Generator

Generates synthetic money-transfer data and populates the money_transfers
feature group in Hopsworks. The amount distribution models realistic US
domestic transfer patterns, including a structuring cluster just below the
$10,000 BSA/CTR reporting threshold.

Usage:
    python ccfraud/synth_money_transfers.py
    python ccfraud/synth_money_transfers.py --num-users 5000 --num-transfers 500000
    python ccfraud/synth_money_transfers.py --start-date 2025-09-01 --end-date 2026-01-01
"""

import argparse
from datetime import datetime, timedelta

import numpy as np
import polars as pl
import hopsworks
from hsfs.feature import Feature


# ---------------------------------------------------------------------------
# CLI arguments
# ---------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate synthetic money-transfer data"
    )
    parser.add_argument("--num-users", type=int, default=2000,
                        help="Number of unique user IDs (default: 2000)")
    parser.add_argument("--num-banks", type=int, default=50,
                        help="Number of banks (default: 50)")
    parser.add_argument("--num-transfers", type=int, default=200_000,
                        help="Number of transfers to generate (default: 200000)")
    parser.add_argument("--start-date", type=str, default=None,
                        help="Start date YYYY-MM-DD (default: 90 days ago)")
    parser.add_argument("--end-date", type=str, default=None,
                        help="End date YYYY-MM-DD (default: today)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed (default: 42)")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Amount distribution
# ---------------------------------------------------------------------------
def generate_transfer_amounts(rng: np.random.Generator, n: int) -> np.ndarray:
    """Generate realistic US money-transfer amounts (up to $1 M).

    Mixture distribution modeled on domestic US transfer patterns:
      50 %  small P2P  (Venmo / Zelle)           $5 - $500
      25 %  medium     (rent, bills, tuition)   $500 - $5,000
      12 %  large      (car, down-payment)    $5,000 - $50,000
       3 %  very large (real estate, business) $50,000 - $1,000,000
      10 %  structuring (just below $10 K CTR)  $8,000 - $9,999
    """
    cat = rng.choice(5, size=n, p=[0.50, 0.25, 0.12, 0.03, 0.10])
    amounts = np.empty(n, dtype=np.float64)

    # Small P2P - median ~ $33
    m = cat == 0
    amounts[m] = np.clip(rng.lognormal(3.5, 1.0, m.sum()), 5, 500)

    # Medium - median ~ $1,097
    m = cat == 1
    amounts[m] = np.clip(rng.lognormal(7.0, 0.5, m.sum()), 500, 5_000)

    # Large - median ~ $8,103
    m = cat == 2
    amounts[m] = np.clip(rng.lognormal(9.0, 0.7, m.sum()), 5_000, 50_000)

    # Very large - median ~ $59,874
    m = cat == 3
    amounts[m] = np.clip(rng.lognormal(11.0, 0.8, m.sum()), 50_000, 1_000_000)

    # Structuring - uniform just below $10 K threshold
    m = cat == 4
    amounts[m] = rng.uniform(8_000, 9_999, m.sum())

    return np.round(amounts, 2)


# ---------------------------------------------------------------------------
# Data generation
# ---------------------------------------------------------------------------
def generate_money_transfers(
    num_users: int,
    num_banks: int,
    num_transfers: int,
    start_date: datetime,
    end_date: datetime,
    seed: int = 42,
) -> pl.DataFrame:
    """Return a Polars DataFrame of synthetic money transfers."""
    rng = np.random.default_rng(seed)
    total_seconds = max(int((end_date - start_date).total_seconds()), 1)

    print(f"Generating {num_transfers:,} money transfers "
          f"({num_users} users, {num_banks} banks) ...")

    user_ids = np.array([f"USR_{i:08d}" for i in range(num_users)])
    bank_ids = np.array([f"BNK_{i:04d}" for i in range(num_banks)])

    # Sender / receiver (guaranteed different)
    sender_idx = rng.integers(0, num_users, size=num_transfers)
    receiver_idx = rng.integers(0, num_users - 1, size=num_transfers)
    receiver_idx = np.where(
        receiver_idx >= sender_idx, receiver_idx + 1, receiver_idx
    )

    # Bank assignments
    sender_bank_idx = rng.integers(0, num_banks, size=num_transfers)
    receiver_bank_idx = rng.integers(0, num_banks, size=num_transfers)

    # Amounts
    amounts = generate_transfer_amounts(rng, num_transfers)

    # Currency: 90 % USD, 5 % EUR, 3 % GBP, 2 % CAD
    curr_pool = np.array(["USD", "EUR", "GBP", "CAD"])
    currencies = curr_pool[
        rng.choice(4, size=num_transfers, p=[0.90, 0.05, 0.03, 0.02])
    ]

    # Timestamps - uniform random within the date range
    offsets = rng.integers(0, total_seconds, size=num_transfers)
    event_times = [start_date + timedelta(seconds=int(s)) for s in offsets]

    df = pl.DataFrame({
        "transfer_id": np.arange(num_transfers, dtype=np.int64),
        "sender_id": user_ids[sender_idx].tolist(),
        "receiver_id": user_ids[receiver_idx].tolist(),
        "amount": amounts,
        "sender_bank_id": bank_ids[sender_bank_idx].tolist(),
        "receiver_bank_id": bank_ids[receiver_bank_idx].tolist(),
        "currency": currencies.tolist(),
        "event_time": event_times,
    })

    near = (df["amount"] >= 8_000) & (df["amount"] < 10_000)
    print(f"  Near-threshold ($8K-$10K): {near.sum():,} transfers "
          f"({near.mean() * 100:.1f}%)")
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    args = parse_args()

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
        start_date = end_date - timedelta(days=90)

    print(f"Date range: {start_date.date()} to {end_date.date()}")

    # Generate data
    df = generate_money_transfers(
        num_users=args.num_users,
        num_banks=args.num_banks,
        num_transfers=args.num_transfers,
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
        name="money_transfers",
        version=1,
        description=(
            "Synthetic money transfers between users. Includes transfer "
            "amount, sender/receiver IDs, bank IDs, and currency."
        ),
        primary_key=["transfer_id"],
        event_time="event_time",
        features=[
            Feature("transfer_id", type="bigint"),
            Feature("sender_id", type="string"),
            Feature("receiver_id", type="string"),
            Feature("amount", type="double"),
            Feature("sender_bank_id", type="string"),
            Feature("receiver_bank_id", type="string"),
            Feature("currency", type="string"),
            Feature("event_time", type="timestamp"),
        ],
    )

    try:
        fg.save()
    except Exception:
        print("Feature group money_transfers already exists")

    # Insert data
    print(f"\nInserting {len(df):,} rows into money_transfers feature group...")
    fg.insert(df)

    print("\nDone! money_transfers feature group populated.")


if __name__ == "__main__":
    main()
