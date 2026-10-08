#!/usr/bin/env python3
"""
Backfill of the sliding-window card aggregates (cc_trans_aggs_fg) with Polars.

Computes, over the offline credit_card_transactions table, the same per-card hopping
windows (1 hour long, sliding every minute) as the Spark Structured Streaming job, and
writes to cc_trans_aggs_fg:

  offline  for every transaction in the history, the window a point-in-time join selects
           for it: the card's latest window that ended at or before the transaction. That
           is all the training pipeline can ever read (the feature view joins each
           transaction with exactly that row), and it is at most one row per transaction
           instead of the ~60 windows every transaction falls into.
  online   each card's latest closed window, which is all the online store keeps.

Runs as a PYTHON job (python-feature-pipeline), in card batches so the full set of windows
is never held in memory. A re-run upserts on (cc_num, event_time), so an overlapping range
is idempotent.

    python ccfraud/jobs.py start backfill-aggs --wait
    python ccfraud/2b-backfill-aggs-pipeline.py --start-date 2026-09-01 --end-date 2026-09-30
"""

import argparse
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import hopsworks
import polars as pl

# __file__ is ccfraud/ccfraud/2b-backfill-aggs-pipeline.py; a Hopsworks job runs it in place
current_file = Path(__file__).absolute()
ccfraud_project_dir = current_file.parent.parent  # ccfraud/
root_dir = ccfraud_project_dir.parent  # mlfs-book/
sys.path.insert(0, str(root_dir))
sys.path.insert(0, str(ccfraud_project_dir))

from ccfraud.features import cc_trans_aggs_fg as aggs  # noqa: E402
from ccfraud.features.common import ensure_statistics_disabled  # noqa: E402


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Backfill cc_trans_aggs_fg from the transaction history (Polars)")
    parser.add_argument("--start-date", default=None, help="First day of transactions to aggregate, YYYY-MM-DD")
    parser.add_argument("--end-date", default=None,
                        help="Last day of transactions to aggregate (inclusive), YYYY-MM-DD (default: now)")
    parser.add_argument("--cards-per-batch", type=int, default=100,
                        help="Cards aggregated at a time; every transaction falls into ~60 windows, so a "
                             "batch holds ~60 x its transactions in memory (default: 100)")
    parser.add_argument("--skip-online", action="store_true",
                        help="Do not write each card's latest window to the online store")
    parser.add_argument("--wait", action="store_true",
                        help="Wait for the offline materialization job of the feature group to finish")
    parser.add_argument("--env-file", default=None,
                        help="Path to .env file, when running outside Hopsworks (default: <root>/.env)")
    return parser.parse_args(argv)


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def read_transactions(trans_fg, start: datetime | None, cutoff: datetime) -> pl.DataFrame:
    query = trans_fg.select(aggs.TRANSACTION_COLUMNS).filter(trans_fg.ts < cutoff)
    if start is not None:
        query = query.filter(trans_fg.ts >= start)
    df = query.read(dataframe_type="polars")
    if df.schema["ts"].time_zone is not None:
        df = df.with_columns(pl.col("ts").dt.convert_time_zone("UTC").dt.replace_time_zone(None))
    return df.with_columns(pl.col("ts").cast(pl.Datetime("us")))


def backfill(transactions: pl.DataFrame, cutoff: datetime, cards_per_batch: int):
    """(offline rows, online rows) of the aggregates, computed one batch of cards at a time."""
    cc_nums = transactions["cc_num"].unique().sort()
    offline, online = [], []
    t_start = time.monotonic()
    for i in range(0, cc_nums.len(), cards_per_batch):
        batch_cards = cc_nums.slice(i, cards_per_batch)
        batch = transactions.filter(pl.col("cc_num").is_in(batch_cards.to_list()))
        # Only closed windows: a window that ends after the cutoff is still collecting transactions
        windows = aggs.window_aggregates(batch).filter(pl.col("event_time") <= cutoff)
        probes = pl.concat([
            batch.select("cc_num", "ts").with_columns(pl.lit(False).alias("_latest")),
            pl.DataFrame({"cc_num": batch_cards}).with_columns(
                pl.lit(cutoff).cast(pl.Datetime("us")).alias("ts"), pl.lit(True).alias("_latest")),
        ])
        selected = aggs.lookup_windows(windows, probes)
        offline.append(selected.filter(~pl.col("_latest")).unique(subset=["cc_num", "event_time"]).drop("ts", "_latest"))
        online.append(selected.filter(pl.col("_latest")).drop("ts", "_latest"))
        done = min(i + cards_per_batch, cc_nums.len())
        print(f"  {done}/{cc_nums.len()} cards, {sum(f.height for f in offline):,} windows "
              f"({time.monotonic() - t_start:.0f}s)")
    return pl.concat(offline), pl.concat(online)


def main(argv=None):
    # Line-buffered stdout: as a Hopsworks job stdout is a file, where Python would otherwise
    # hold progress lines back for hours until its block buffer fills
    sys.stdout.reconfigure(line_buffering=True)
    args = parse_args(argv)
    env_file = Path(args.env_file) if args.env_file else root_dir / ".env"
    if env_file.exists():
        from mlfs import config
        config.HopsworksSettings(_env_file=str(env_file))

    project = hopsworks.login()
    fs = project.get_feature_store()
    trans_fg = fs.get_feature_group("credit_card_transactions", version=1)
    card_fg = fs.get_feature_group("card_details", version=1)
    if trans_fg is None or card_fg is None:
        sys.exit("credit_card_transactions / card_details not found: run `inv datamart` first")
    aggs_fg = aggs.get_or_create_aggs_fg(fs, parents=[trans_fg, card_fg])
    ensure_statistics_disabled(aggs_fg)

    cutoff = aggs.floor_minute(utcnow())
    if args.end_date:
        cutoff = min(cutoff, datetime.strptime(args.end_date, "%Y-%m-%d") + timedelta(days=1))
    start = datetime.strptime(args.start_date, "%Y-%m-%d") if args.start_date else None
    print(f"Aggregating transactions {'from ' + str(start) + ' ' if start else ''}up to {cutoff} UTC")
    transactions = read_transactions(trans_fg, start, cutoff)
    print(f"Read {transactions.height:,} transactions of {transactions['cc_num'].n_unique():,} cards")
    if transactions.height == 0:
        print("Nothing to aggregate")
        return

    cards = card_fg.select(["cc_num", "bank_id", "last_modified"]).read(dataframe_type="polars")
    offline, online = backfill(transactions, cutoff, args.cards_per_batch)
    offline = aggs.with_bank_ids(offline, cards)
    online = aggs.with_bank_ids(online, cards)

    # Offline: the windows the point-in-time join selects, for the training pipeline. The rows
    # go through the feature group's topic marked for the offline store only; the feature
    # group's materialization job then writes them to the Delta table.
    print(f"Writing {offline.height:,} windows to the offline store")
    aggs_fg.insert(offline, storage="offline", write_options={"wait_for_job": args.wait})

    if not args.skip_online:
        print(f"Writing the latest window of {online.height:,} cards to the online store")
        aggs_fg.insert(online, storage="online")
    print("Backfill completed")


if __name__ == "__main__":
    main()
