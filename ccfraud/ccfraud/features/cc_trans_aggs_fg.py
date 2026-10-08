"""
Sliding-window card aggregates (cc_trans_aggs_fg) in Polars.

The features are per-card aggregates over a hopping event-time window: WINDOW_LENGTH_MINS
long, sliding every SLIDE_MINS. Each row describes the window [event_time - 1 hour,
event_time) where event_time is the window end, and a window with no transactions emits
no row. This module owns the feature group's schema; the Spark Structured Streaming job
(2-spark-streaming-feature-pipeline.py) computes the same windows live, and the Polars
backfill job (2b-backfill-aggs-pipeline.py) computes them over the transaction history.

Windows are minute-aligned [start, start + WINDOW), labelled by their end, and a window with
no transactions emits no row: the same windows as Spark's F.window(ts, WINDOW, SLIDE).
"""

from datetime import datetime

import polars as pl

AGGS_FG_NAME = "cc_trans_aggs_fg"
# Version 2: sliding-window features (version 1 held Feldera's rolling aggregates)
AGGS_FG_VERSION = 2

WINDOW_LENGTH_MINS = 60
SLIDE_MINS = 1
SHORT_WINDOW_MINS = 10

# (name, type, description) of every feature, in the feature group's column order
AGGS_SCHEMA = [
    ("cc_num", "string", "Credit card number"),
    ("event_time", "timestamp", f"End of the {WINDOW_LENGTH_MINS}-minute window (exclusive) the aggregates cover"),
    ("account_id", "string", "Account that owns the card"),
    ("bank_id", "string", "Bank that issued the card"),
    ("num_trans_last_10_mins", "bigint", f"Number of transactions in the {SHORT_WINDOW_MINS} minutes before event_time"),
    ("sum_trans_last_10_mins", "double",
     f"Total amount of transactions in the {SHORT_WINDOW_MINS} minutes before event_time"),
    ("num_trans_last_hour", "bigint", "Number of transactions in the hour before event_time"),
    ("sum_trans_last_hour", "double", "Total amount of transactions in the hour before event_time"),
    ("max_trans_last_hour", "double", "Largest transaction amount in the hour before event_time"),
    ("num_ip_addresses_last_hour", "bigint", "Number of distinct IP addresses used in the hour before event_time"),
    ("prev_ts", "timestamp", "Time of the card's last transaction before event_time"),
    ("prev_ip_address", "string", "IP address of the card's last transaction before event_time"),
    ("prev_card_present", "boolean", "Whether the card was present in its last transaction before event_time"),
]
AGGS_COLUMNS = [name for name, _, _ in AGGS_SCHEMA]

# Columns of credit_card_transactions the aggregates are computed from
TRANSACTION_COLUMNS = ["cc_num", "account_id", "amount", "ip_address", "card_present", "ts"]


def window_aggregates(transactions: pl.DataFrame) -> pl.DataFrame:
    """Every non-empty (card, window) of `transactions`, one row per window, without bank_id.

    transactions: TRANSACTION_COLUMNS, ts a naive UTC datetime.

    A transaction at ts falls into the WINDOW_LENGTH_MINS / SLIDE_MINS windows whose end e
    satisfies ts < e <= ts + WINDOW_LENGTH_MINS, i.e. e = truncate(ts, SLIDE) + k * SLIDE for
    k = 1..WINDOW/SLIDE. Each transaction is expanded into those window ends and the
    windows aggregated, as Spark's F.window(ts, WINDOW, SLIDE) does.
    """
    n_windows = WINDOW_LENGTH_MINS // SLIDE_MINS
    exploded = (
        transactions.select(TRANSACTION_COLUMNS)
        .with_columns(pl.col("ts").cast(pl.Datetime("us")))
        .with_columns(pl.int_ranges(1, n_windows + 1).alias("_k"))
        .explode("_k")
        .with_columns(
            (pl.col("ts").dt.truncate(f"{SLIDE_MINS}m") + pl.duration(minutes=pl.col("_k") * SLIDE_MINS))
            .alias("event_time")
        )
    )
    # The last SHORT_WINDOW_MINS of the window, i.e. just before its end
    in_short = pl.col("ts") >= pl.col("event_time") - pl.duration(minutes=SHORT_WINDOW_MINS)
    return exploded.group_by(["cc_num", "event_time"]).agg(
        pl.col("account_id").drop_nulls().first().alias("account_id"),
        in_short.sum().cast(pl.Int64).alias("num_trans_last_10_mins"),
        pl.col("amount").filter(in_short).sum().alias("sum_trans_last_10_mins"),
        pl.len().cast(pl.Int64).alias("num_trans_last_hour"),
        pl.col("amount").sum().alias("sum_trans_last_hour"),
        pl.col("amount").max().alias("max_trans_last_hour"),
        pl.col("ip_address").drop_nulls().n_unique().cast(pl.Int64).alias("num_ip_addresses_last_hour"),
        pl.col("ts").max().alias("prev_ts"),
        pl.col("ip_address").sort_by("ts").last().alias("prev_ip_address"),
        pl.col("card_present").sort_by("ts").last().alias("prev_card_present"),
    )


def lookup_windows(windows: pl.DataFrame, probes: pl.DataFrame) -> pl.DataFrame:
    """For each probe (cc_num, ts) the window a point-in-time join selects: the card's latest
    window with event_time <= ts. Probes with no such window are dropped. Returns the probes'
    columns plus the window's.

    A window ending at or before ts never contains the transaction at ts itself (windows are
    closed on the left, open on the right), so a transaction never leaks into its own features.
    """
    matched = probes.sort("ts").join_asof(
        windows.sort("event_time"),
        left_on="ts",
        right_on="event_time",
        by="cc_num",
        strategy="backward",
        coalesce=False,
    )
    return matched.drop_nulls("event_time")


def with_bank_ids(windows: pl.DataFrame, cards: pl.DataFrame) -> pl.DataFrame:
    """Add bank_id from card_details (cc_num, bank_id, last_modified; latest row per card wins)
    and order the columns as the feature group's schema."""
    latest = cards.sort("last_modified").unique(subset=["cc_num"], keep="last").select(["cc_num", "bank_id"])
    return windows.join(latest, on="cc_num", how="left").select(AGGS_COLUMNS)


def get_or_create_aggs_fg(fs, parents):
    """The cc_trans_aggs_fg feature group, created (and saved) if it does not exist.

    Statistics are off: with the Python engine every insert would otherwise launch a PySpark
    statistics job.
    """
    from hsfs.feature import Feature

    fg = fs.get_or_create_feature_group(
        name=AGGS_FG_NAME,
        version=AGGS_FG_VERSION,
        description=(f"Per-card transaction aggregates over a {WINDOW_LENGTH_MINS}-minute window sliding every "
                     f"{SLIDE_MINS} minute: live from a Spark Structured Streaming job, history from a Polars "
                     "backfill. event_time is the window end."),
        primary_key=["cc_num"],
        event_time="event_time",
        online_enabled=True,
        stream=True,
        features=[Feature(name, type=type_, description=desc) for name, type_, desc in AGGS_SCHEMA],
        parents=parents,
        statistics_config=False,
    )
    if fg.id is None:
        fg.save()
        print(f"Created feature group {AGGS_FG_NAME} v{AGGS_FG_VERSION}")
    return fg


def floor_minute(ts: datetime) -> datetime:
    return ts.replace(second=0, microsecond=0)
