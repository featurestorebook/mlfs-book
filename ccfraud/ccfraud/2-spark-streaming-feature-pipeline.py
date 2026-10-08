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

This is the only Spark job of the example. The feature group, its schema and the
backfill of the windows over the transaction history belong to the Polars job
2b-backfill-aggs-pipeline.py (ccfraud/features/cc_trans_aggs_fg.py), which runs first.
A new stream starts at the topic's latest offset (history is the backfill's job), so
for its first hour a window misses transactions from before the start.

    hops job deploy ccfraud-streaming-aggs <path>/ccfraud/2-spark-streaming-feature-pipeline.py \
        --type pyspark --env spark-feature-pipeline --args "--mode stream" --run
"""

import argparse
import sys
from datetime import datetime, timezone

import hopsworks
from hsfs.core.storage_connector_api import StorageConnectorApi
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

AGGS_FG_NAME = "cc_trans_aggs_fg"
# Version 2: sliding-window features (version 1 held Feldera's rolling aggregates)
AGGS_FG_VERSION = 2

# Must match ccfraud/features/cc_trans_aggs_fg.py, which the backfill computes with
WINDOW_LENGTH_MINS = 60
SLIDE_MINS = 1
SHORT_WINDOW_MINS = 10
SHUFFLE_PARTITIONS = 4

# Quartz cron for the aggs feature group's offline materialization (streamed rows reach
# the online store immediately and the offline store when this job runs).
DEFAULT_MATERIALIZATION_CRON = "0 0 * ? * *"  # hourly


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Sliding-window card aggregates with Spark Structured Streaming")
    parser.add_argument("--mode", choices=["stream"], default="stream",
                        help="Kept for compatibility: the backfill is the Polars job 2b-backfill-aggs-pipeline.py")
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
    return parser.parse_args(argv)


def window_aggregates(transactions: DataFrame, cards: DataFrame, columns: list[str]) -> DataFrame:
    """Per-card aggregates over a sliding event-time window, in the feature group's column order.

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
        .withColumn("event_time", F.col("window.end"))
        .select(*columns)
    )


def latest_cards(card_fg) -> DataFrame:
    """cc_num -> bank_id, from the latest version of each card in card_details."""
    cards = card_fg.select(["cc_num", "bank_id", "last_modified"]).read()
    latest = cards.groupBy("cc_num").agg(F.max_by("bank_id", "last_modified").alias("bank_id"))
    return latest


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

    aggs = window_aggregates(transactions, cards, [f.name for f in aggs_fg.features])

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


def main(argv=None):
    # Line-buffered stdout: as a Hopsworks job stdout is a file, where Python would otherwise
    # hold progress lines back for hours until its block buffer fills
    sys.stdout.reconfigure(line_buffering=True)
    args = parse_args(argv)
    project = hopsworks.login()
    fs = project.get_feature_store()
    spark = hopsworks.build_spark("ccfraud-aggs-stream")
    # Number of state partitions: the state store writes ~7 files per partition per micro-batch
    # to the checkpoint dir, so keep it small at this volume (a few hundred cards a minute).
    # Spark fixes it at the first run of a checkpoint: to change it, stop the job, delete the
    # checkpoint dir (the stream then resumes at the topic's latest offset) and restart.
    spark.conf.set("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
    # Session timezone UTC, so window boundaries line up with the UTC timestamps in the feature store
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    trans_fg = fs.get_feature_group("credit_card_transactions", version=1)
    card_fg = fs.get_feature_group("card_details", version=1)
    aggs_fg = fs.get_feature_group(AGGS_FG_NAME, version=AGGS_FG_VERSION)
    if trans_fg is None or card_fg is None or aggs_fg is None:
        sys.exit(f"credit_card_transactions, card_details and {AGGS_FG_NAME} v{AGGS_FG_VERSION} must exist: "
                 "run `inv datamart` and `inv backfill-aggs` first")
    cards = latest_cards(card_fg).cache()
    print(f"Loaded {cards.count()} cards")

    schedule_materialization(aggs_fg, args.materialization_cron)
    run_stream(project, fs, spark, trans_fg, aggs_fg, cards, args)


if __name__ == "__main__":
    main()
