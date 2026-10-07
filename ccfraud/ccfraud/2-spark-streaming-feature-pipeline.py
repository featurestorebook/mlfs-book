#!/usr/bin/env python
"""
Streaming feature pipeline: Spark Structured Streaming on Hopsworks

Reads credit card transactions from the Kafka topic of the `credit_card_transactions`
feature group and computes per-card aggregates over a sliding (hopping) event-time
window: 1 hour long, sliding every 1 minute. Results are written to the
`cc_trans_aggs_fg` feature group (online + offline) with `insert_stream`.

Why a sliding window and not a rolling (per-row OVER) aggregation: Spark keeps one
state row per (card, window), and each transaction falls into WINDOW_LENGTH / SLIDE
= 60 windows. A window is finalized and emitted once the watermark passes its end,
after which its state is dropped, so state stays bounded however long the job runs
and the job scales out by partitioning on cc_num. Short-horizon features
(last 10 minutes) are computed inside the same hourly window with conditional
aggregates, so they cost no extra windows or shuffles. Longer horizons (day/week)
do not belong in a 1-minute-slide window (1,440-10,080 copies per row); compute
them in a batch pipeline.

Each output row describes the window [event_time - 1 hour, event_time), where
event_time is the window end. A point-in-time join of a transaction at time `ts`
picks the latest window that ended at or before `ts`, so the transaction itself
never leaks into its own features.

Modes:
  backfill the same aggregation as a batch job over the offline credit_card_transactions
           table: the history the training pipeline needs. Every window goes to the
           offline store, only each card's latest window to the online store.
  stream   (default) the continuous Structured Streaming job, run as a PYSPARK job. A new
           stream starts at the topic's latest offset (history is the backfill's job),
           so for its first hour a window misses transactions from before the start.

    hops job deploy ccfraud-backfill-aggs <path>/ccfraud/2-spark-streaming-feature-pipeline.py \
        --type pyspark --env spark-feature-pipeline --args "--mode backfill" --run --wait
    hops job deploy ccfraud-streaming-aggs <path>/ccfraud/2-spark-streaming-feature-pipeline.py \
        --type pyspark --env spark-feature-pipeline --args "--mode stream" --run
"""

import argparse
from datetime import datetime, timedelta, timezone

import hopsworks
from hsfs.core.storage_connector_api import StorageConnectorApi
from hsfs.feature import Feature
from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F

AGGS_FG_NAME = "cc_trans_aggs_fg"
# Version 2: sliding-window features from Spark (version 1 held Feldera's rolling aggregates)
AGGS_FG_VERSION = 2

WINDOW_LENGTH_MINS = 60
SLIDE_MINS = 1
SHORT_WINDOW_MINS = 10

AGGS_FEATURES = [
    Feature("cc_num", type="string", description="Credit card number"),
    Feature("event_time", type="timestamp",
            description=f"End of the {WINDOW_LENGTH_MINS}-minute window (exclusive) the aggregates cover"),
    Feature("account_id", type="string", description="Account that owns the card"),
    Feature("bank_id", type="string", description="Bank that issued the card"),
    Feature("num_trans_last_10_mins", type="bigint",
            description=f"Number of transactions in the {SHORT_WINDOW_MINS} minutes before event_time"),
    Feature("sum_trans_last_10_mins", type="double",
            description=f"Total amount of transactions in the {SHORT_WINDOW_MINS} minutes before event_time"),
    Feature("num_trans_last_hour", type="bigint",
            description="Number of transactions in the hour before event_time"),
    Feature("sum_trans_last_hour", type="double",
            description="Total amount of transactions in the hour before event_time"),
    Feature("max_trans_last_hour", type="double",
            description="Largest transaction amount in the hour before event_time"),
    Feature("num_ip_addresses_last_hour", type="bigint",
            description="Approximate number of distinct IP addresses used in the hour before event_time"),
    Feature("prev_ts", type="timestamp", description="Time of the card's last transaction before event_time"),
    Feature("prev_ip_address", type="string",
            description="IP address of the card's last transaction before event_time"),
    Feature("prev_card_present", type="boolean",
            description="Whether the card was present in its last transaction before event_time"),
]

# Quartz cron for the aggs feature group's offline materialization (streamed rows reach
# the online store immediately and the offline store when this job runs).
DEFAULT_MATERIALIZATION_CRON = "0 0 * ? * *"  # hourly


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Sliding-window card aggregates with Spark Structured Streaming")
    parser.add_argument("--mode", choices=["stream", "backfill"], default="stream")
    parser.add_argument("--starting-offsets", default="latest", choices=["earliest", "latest"],
                        help="Where a new stream (no checkpoint yet) starts reading the topic; a restart "
                             "resumes from its checkpoint (default: latest)")
    parser.add_argument("--watermark", default="2 minutes",
                        help="How late a transaction may arrive and still be counted (default: '2 minutes'). "
                             "A window is emitted this long after it ends.")
    parser.add_argument("--max-offsets-per-trigger", type=int, default=0,
                        help="Cap on Kafka records per micro-batch, 0 for no cap (default: 0)")
    parser.add_argument("--checkpoint-dir", default=None,
                        help="Streaming checkpoint dir (default: Resources/ccfraud/checkpoints/<fg>_<version>)")
    parser.add_argument("--materialization-cron", default=DEFAULT_MATERIALIZATION_CRON,
                        help="Quartz cron for the aggs feature group's offline materialization job; "
                             "'' leaves the schedule unchanged (default: hourly)")
    parser.add_argument("--start-date", default=None, help="Backfill: first day to aggregate, YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="Backfill: last day to aggregate (inclusive), YYYY-MM-DD")
    return parser.parse_args(argv)


def window_aggregates(transactions: DataFrame, cards: DataFrame) -> DataFrame:
    """Per-card aggregates over a sliding event-time window. Works on batch and streaming DataFrames.

    transactions: cc_num, account_id, amount, ip_address, card_present, ts
    cards:        cc_num, bank_id (static; broadcast to every partition)
    """
    window = F.window("ts", f"{WINDOW_LENGTH_MINS} minutes", f"{SLIDE_MINS} minute")
    # Transactions in the last SHORT_WINDOW_MINS of the window, i.e. just before its end
    in_short_window = F.col("ts") >= F.col("window.end") - F.expr(f"INTERVAL {SHORT_WINDOW_MINS} MINUTES")

    return (
        transactions
        .join(F.broadcast(cards), on="cc_num", how="left")
        .withColumn("window", window)
        .groupBy("window", "cc_num")
        .agg(
            F.first("account_id", ignorenulls=True).alias("account_id"),
            F.first("bank_id", ignorenulls=True).alias("bank_id"),
            F.sum(F.when(in_short_window, 1).otherwise(0)).cast("bigint").alias("num_trans_last_10_mins"),
            F.sum(F.when(in_short_window, F.col("amount")).otherwise(0.0)).alias("sum_trans_last_10_mins"),
            F.count("*").alias("num_trans_last_hour"),
            F.sum("amount").alias("sum_trans_last_hour"),
            F.max("amount").alias("max_trans_last_hour"),
            F.approx_count_distinct("ip_address").alias("num_ip_addresses_last_hour"),
            F.max("ts").alias("prev_ts"),
            F.max_by("ip_address", "ts").alias("prev_ip_address"),
            F.max_by("card_present", "ts").alias("prev_card_present"),
        )
        .select(F.col("window.end").alias("event_time"), *[f.name for f in AGGS_FEATURES if f.name != "event_time"])
    )


def latest_cards(card_fg) -> DataFrame:
    """cc_num -> bank_id, from the latest version of each card in card_details."""
    cards = card_fg.select(["cc_num", "bank_id", "last_modified"]).read()
    latest = cards.groupBy("cc_num").agg(F.max_by("bank_id", "last_modified").alias("bank_id"))
    return latest


def get_or_create_aggs_fg(fs, parents):
    fg = fs.get_or_create_feature_group(
        name=AGGS_FG_NAME,
        version=AGGS_FG_VERSION,
        description=(f"Per-card transaction aggregates over a {WINDOW_LENGTH_MINS}-minute window sliding every "
                     f"{SLIDE_MINS} minute, computed by a Spark Structured Streaming job. "
                     "event_time is the window end."),
        primary_key=["cc_num"],
        event_time="event_time",
        online_enabled=True,
        stream=True,
        features=AGGS_FEATURES,
        parents=parents,
        statistics_config=False,
    )
    if fg.id is None:
        fg.save()
        print(f"Created feature group {AGGS_FG_NAME} v{AGGS_FG_VERSION}")
    return fg


def schedule_materialization(fg, cron):
    if not cron:
        return
    try:
        fg.materialization_job.schedule(cron_expression=cron, start_time=datetime.now(timezone.utc))
        print(f"Scheduled offline materialization of {fg.name} with cron '{cron}'")
    except Exception as e:
        print(f"Warning: could not schedule materialization of {fg.name}: {e}")


def run_stream(project, fs, spark, trans_fg, aggs_fg, cards, args):
    kafka = StorageConnectorApi()._get_kafka_connector(fs.id, external=False)
    options = {"startingOffsets": args.starting_offsets, "failOnDataLoss": "false"}
    if args.max_offsets_per_trigger > 0:
        options["maxOffsetsPerTrigger"] = str(args.max_offsets_per_trigger)
    print(f"Reading topic {trans_fg._online_topic_name}")
    transactions = kafka.read_stream(
        topic=trans_fg._online_topic_name,
        message_format="avro",
        schema=trans_fg.avro_schema,
        options=options,
    ).withWatermark("ts", args.watermark)

    aggs = window_aggregates(transactions, cards)

    checkpoint_dir = args.checkpoint_dir or (
        f"/Projects/{project.name}/Resources/ccfraud/checkpoints/{AGGS_FG_NAME}_{AGGS_FG_VERSION}"
    )
    print(f"Starting streaming query, checkpoints in {checkpoint_dir}")
    # append: each window is written once, when the watermark passes its end
    aggs_fg.insert_stream(
        aggs,
        query_name=f"{AGGS_FG_NAME}_{AGGS_FG_VERSION}",
        output_mode="append",
        checkpoint_dir=checkpoint_dir,
        await_termination=True,
    )


def run_backfill(trans_fg, aggs_fg, cards, args):
    # Only windows that have closed: a batch job also emits the windows a recent transaction
    # falls into that end in the future, and those are incomplete.
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None, second=0, microsecond=0)
    query = trans_fg.select(["cc_num", "account_id", "amount", "ip_address", "card_present", "ts"])
    if args.start_date:
        query = query.filter(trans_fg.ts >= datetime.strptime(args.start_date, "%Y-%m-%d"))
    if args.end_date:
        cutoff = min(cutoff, datetime.strptime(args.end_date, "%Y-%m-%d") + timedelta(days=1))
    query = query.filter(trans_fg.ts < cutoff)
    print(f"Aggregating windows that end at or before {cutoff} UTC")
    # The offline Delta table returns ts as timestamp_ntz; the feature group (and the stream) use timestamp
    transactions = query.read().withColumn("ts", F.col("ts").cast("timestamp"))
    aggs = window_aggregates(transactions, cards).filter(F.col("event_time") <= F.lit(cutoff)).persist()

    # Offline: every window, for point-in-time correct training data. A re-run upserts
    # on (cc_num, event_time), so backfilling an overlapping range is idempotent.
    print(f"Writing {aggs.count():,} window aggregates to the offline store")
    aggs_fg.insert(aggs, storage="offline")

    # Online: only each card's latest window, which is all the online store keeps
    latest = (
        aggs.withColumn("_rank", F.row_number().over(Window.partitionBy("cc_num").orderBy(F.desc("event_time"))))
        .filter("_rank = 1")
        .drop("_rank")
    )
    print("Writing each card's latest window to the online store")
    aggs_fg.insert(latest, storage="online")
    aggs.unpersist()


def main(argv=None):
    args = parse_args(argv)
    project = hopsworks.login()
    fs = project.get_feature_store()
    spark = hopsworks.build_spark(f"ccfraud-aggs-{args.mode}")
    spark.conf.set("spark.sql.shuffle.partitions", "16")
    # Session timezone UTC, so window boundaries line up with the UTC timestamps in the feature store
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    trans_fg = fs.get_feature_group("credit_card_transactions", version=1)
    card_fg = fs.get_feature_group("card_details", version=1)
    aggs_fg = get_or_create_aggs_fg(fs, parents=[trans_fg, card_fg])
    cards = latest_cards(card_fg).cache()
    print(f"Loaded {cards.count()} cards")

    if args.mode == "backfill":
        run_backfill(trans_fg, aggs_fg, cards, args)
    else:
        schedule_materialization(aggs_fg, args.materialization_cron)
        run_stream(project, fs, spark, trans_fg, aggs_fg, cards, args)


if __name__ == "__main__":
    main()
