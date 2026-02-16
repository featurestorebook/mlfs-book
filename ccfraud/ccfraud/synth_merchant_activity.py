#!/usr/bin/env python3
"""
Synthetic Merchant Activity Generator

Generates three synthetic tables for merchant activity drop detection:
  1. merchant_activity  — daily transaction counts/volumes per merchant
  2. terminal_events    — terminal online/offline/maintenance/error events
  3. support_tickets    — merchant support tickets

~20% of merchants have 1-2 "drop periods" where volume falls to 0-10% of
baseline, simulating real-world merchant outages or churn signals.

Usage:
    python ccfraud/synth_merchant_activity.py
    python ccfraud/synth_merchant_activity.py --num-merchants 50
    python ccfraud/synth_merchant_activity.py --start-date 2025-08-01 --end-date 2026-02-01
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
        description="Generate synthetic merchant activity data"
    )
    parser.add_argument("--num-merchants", type=int, default=200,
                        help="Number of merchants (default: 200)")
    parser.add_argument("--start-date", type=str, default=None,
                        help="Start date YYYY-MM-DD (default: 180 days ago)")
    parser.add_argument("--end-date", type=str, default=None,
                        help="End date YYYY-MM-DD (default: today)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed (default: 42)")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Table 1: merchant_activity  (one row per merchant per day)
# ---------------------------------------------------------------------------
def generate_merchant_activity(
    num_merchants: int,
    start_date: datetime,
    end_date: datetime,
    rng: np.random.Generator,
) -> tuple[pl.DataFrame, dict, dict]:
    """Return (DataFrame, merchant→terminals map, drop_merchant set)."""

    num_days = (end_date.date() - start_date.date()).days + 1
    merchant_ids = [f"MER_{i:06d}" for i in range(num_merchants)]

    # Assign 1-3 terminals per merchant
    merchant_terminals: dict[str, list[str]] = {}
    terminal_counter = 0
    for mid in merchant_ids:
        n_terminals = rng.integers(1, 4)  # 1..3
        terminals = [f"TRM_{terminal_counter + j:06d}" for j in range(n_terminals)]
        merchant_terminals[mid] = terminals
        terminal_counter += len(terminals)

    # Baseline volume per merchant (lognormal)
    base_volume = rng.lognormal(mean=7.0, sigma=1.0, size=num_merchants)
    base_count = np.clip(rng.lognormal(mean=3.5, sigma=0.8, size=num_merchants), 5, 500).astype(int)

    # Select ~20% as drop merchants
    is_drop = rng.random(num_merchants) < 0.20
    drop_merchants: dict[str, list[tuple[int, int]]] = {}
    for idx in np.where(is_drop)[0]:
        n_drops = rng.integers(1, 3)  # 1..2
        periods = []
        for _ in range(n_drops):
            drop_start = rng.integers(35, max(36, num_days - 14))
            drop_len = rng.integers(3, 15)  # 3..14 days
            periods.append((int(drop_start), int(drop_start + drop_len)))
        drop_merchants[merchant_ids[idx]] = periods

    rows = []
    for i, mid in enumerate(merchant_ids):
        terminals = merchant_terminals[mid]
        drop_periods = drop_merchants.get(mid, [])
        for day_offset in range(num_days):
            day_dt = (start_date + timedelta(days=day_offset)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            # Check if this day falls in a drop period
            in_drop = any(s <= day_offset < e for s, e in drop_periods)
            if in_drop:
                scale = rng.uniform(0.0, 0.10)
            else:
                scale = rng.uniform(0.8, 1.2)

            txn_count = max(0, int(base_count[i] * scale + rng.normal(0, 2)))
            txn_volume = round(max(0.0, base_volume[i] * scale * rng.uniform(0.9, 1.1)), 2)
            terminal_id = rng.choice(terminals)

            rows.append((mid, day_dt, txn_count, txn_volume, terminal_id))

    df = pl.DataFrame(
        {
            "merchant_id": [r[0] for r in rows],
            "event_time": [r[1] for r in rows],
            "txn_count": [r[2] for r in rows],
            "txn_volume": [r[3] for r in rows],
            "terminal_id": [r[4] for r in rows],
        },
        schema={
            "merchant_id": pl.Utf8,
            "event_time": pl.Datetime,
            "txn_count": pl.Int64,
            "txn_volume": pl.Float64,
            "terminal_id": pl.Utf8,
        },
    )

    print(f"  merchant_activity: {len(df):,} rows  "
          f"({num_merchants} merchants × {num_days} days)")
    print(f"  Drop merchants: {len(drop_merchants)}")
    return df, merchant_terminals, drop_merchants


# ---------------------------------------------------------------------------
# Table 2: terminal_events
# ---------------------------------------------------------------------------
def generate_terminal_events(
    merchant_terminals: dict[str, list[str]],
    drop_merchants: dict[str, list[tuple[int, int]]],
    start_date: datetime,
    end_date: datetime,
    rng: np.random.Generator,
) -> pl.DataFrame:
    """One event per terminal per occurrence, ~Poisson(20) events/terminal."""

    total_seconds = max(int((end_date - start_date).total_seconds()), 1)
    event_types_normal = ["online", "offline", "maintenance", "error"]
    event_types_drop = ["offline", "offline", "offline", "error", "maintenance", "online"]

    rows = []
    for mid, terminals in merchant_terminals.items():
        is_drop = mid in drop_merchants
        for tid in terminals:
            n_events = rng.poisson(20)
            for _ in range(n_events):
                offset = int(rng.integers(0, total_seconds))
                evt_time = start_date + timedelta(seconds=offset)
                if is_drop:
                    etype = rng.choice(event_types_drop)
                else:
                    etype = rng.choice(event_types_normal)
                rows.append((tid, mid, etype, evt_time))

    df = pl.DataFrame(
        {
            "terminal_id": [r[0] for r in rows],
            "merchant_id": [r[1] for r in rows],
            "event_type": [r[2] for r in rows],
            "event_time": [r[3] for r in rows],
        },
        schema={
            "terminal_id": pl.Utf8,
            "merchant_id": pl.Utf8,
            "event_type": pl.Utf8,
            "event_time": pl.Datetime,
        },
    )

    print(f"  terminal_events: {len(df):,} rows")
    return df


# ---------------------------------------------------------------------------
# Table 3: support_tickets
# ---------------------------------------------------------------------------
def generate_support_tickets(
    merchant_ids: list[str],
    drop_merchants: dict[str, list[tuple[int, int]]],
    start_date: datetime,
    end_date: datetime,
    rng: np.random.Generator,
) -> pl.DataFrame:
    """~Poisson(5) tickets/merchant; drop merchants get 5-15 extra."""

    total_seconds = max(int((end_date - start_date).total_seconds()), 1)
    ticket_types_normal = ["volume_issue", "terminal_problem", "billing", "general"]
    ticket_types_drop = ["volume_issue", "volume_issue", "terminal_problem",
                         "terminal_problem", "billing", "general"]
    priorities = ["low", "medium", "high"]
    priorities_drop = ["medium", "high", "high"]

    rows = []
    ticket_id = 0
    for mid in merchant_ids:
        is_drop = mid in drop_merchants
        n_base = rng.poisson(5)
        n_extra = rng.integers(5, 16) if is_drop else 0

        for _ in range(n_base):
            offset = int(rng.integers(0, total_seconds))
            evt_time = start_date + timedelta(seconds=offset)
            rows.append((
                ticket_id, mid,
                rng.choice(ticket_types_normal),
                rng.choice(priorities),
                evt_time,
            ))
            ticket_id += 1

        for _ in range(n_extra):
            offset = int(rng.integers(0, total_seconds))
            evt_time = start_date + timedelta(seconds=offset)
            rows.append((
                ticket_id, mid,
                rng.choice(ticket_types_drop),
                rng.choice(priorities_drop),
                evt_time,
            ))
            ticket_id += 1

    df = pl.DataFrame(
        {
            "ticket_id": [r[0] for r in rows],
            "merchant_id": [r[1] for r in rows],
            "ticket_type": [r[2] for r in rows],
            "priority": [r[3] for r in rows],
            "event_time": [r[4] for r in rows],
        },
        schema={
            "ticket_id": pl.Int64,
            "merchant_id": pl.Utf8,
            "ticket_type": pl.Utf8,
            "priority": pl.Utf8,
            "event_time": pl.Datetime,
        },
    )

    print(f"  support_tickets: {len(df):,} rows")
    return df


# ---------------------------------------------------------------------------
# Hopsworks helpers
# ---------------------------------------------------------------------------
def upsert_feature_group(fs, name, description, primary_key, features, df):
    """Create-or-get a feature group, save schema, then insert data."""
    fg = fs.get_or_create_feature_group(
        name=name,
        version=1,
        description=description,
        primary_key=primary_key,
        event_time="event_time",
        features=features,
    )
    try:
        fg.save()
    except Exception:
        print(f"  Feature group {name} already exists")

    print(f"  Inserting {len(df):,} rows into {name}...")
    fg.insert(df)


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
        start_date = end_date - timedelta(days=180)

    rng = np.random.default_rng(args.seed)

    print(f"Date range: {start_date.date()} to {end_date.date()}")
    print(f"Merchants: {args.num_merchants}\n")

    # Generate all 3 tables
    merchant_ids = [f"MER_{i:06d}" for i in range(args.num_merchants)]

    print("Generating merchant_activity...")
    activity_df, merchant_terminals, drop_merchants = generate_merchant_activity(
        num_merchants=args.num_merchants,
        start_date=start_date,
        end_date=end_date,
        rng=rng,
    )

    print("\nGenerating terminal_events...")
    terminal_df = generate_terminal_events(
        merchant_terminals=merchant_terminals,
        drop_merchants=drop_merchants,
        start_date=start_date,
        end_date=end_date,
        rng=rng,
    )

    print("\nGenerating support_tickets...")
    tickets_df = generate_support_tickets(
        merchant_ids=merchant_ids,
        drop_merchants=drop_merchants,
        start_date=start_date,
        end_date=end_date,
        rng=rng,
    )

    # Connect to Hopsworks
    print("\nConnecting to Hopsworks...")
    project = hopsworks.login()
    fs = project.get_feature_store()

    # Insert all 3 feature groups
    print("\n--- merchant_activity ---")
    upsert_feature_group(
        fs, "merchant_activity",
        "Daily transaction counts and volumes per merchant",
        ["merchant_id"],
        [
            Feature("merchant_id", type="string"),
            Feature("event_time", type="timestamp"),
            Feature("txn_count", type="bigint"),
            Feature("txn_volume", type="double"),
            Feature("terminal_id", type="string"),
        ],
        activity_df,
    )

    print("\n--- terminal_events ---")
    upsert_feature_group(
        fs, "terminal_events",
        "Terminal online/offline/maintenance/error events per terminal",
        ["terminal_id"],
        [
            Feature("terminal_id", type="string"),
            Feature("merchant_id", type="string"),
            Feature("event_type", type="string"),
            Feature("event_time", type="timestamp"),
        ],
        terminal_df,
    )

    print("\n--- support_tickets ---")
    upsert_feature_group(
        fs, "support_tickets",
        "Merchant support tickets with type and priority",
        ["ticket_id"],
        [
            Feature("ticket_id", type="bigint"),
            Feature("merchant_id", type="string"),
            Feature("ticket_type", type="string"),
            Feature("priority", type="string"),
            Feature("event_time", type="timestamp"),
        ],
        tickets_df,
    )

    print("\nDone! 3 feature groups populated: "
          "merchant_activity, terminal_events, support_tickets.")


if __name__ == "__main__":
    main()
