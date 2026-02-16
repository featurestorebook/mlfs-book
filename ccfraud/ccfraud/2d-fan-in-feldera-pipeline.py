#!/usr/bin/env python3
"""
Fan-In Detection - Feldera Streaming Feature Pipeline

Reads from the money_transfers feature group (via Kafka), computes fan-in
structuring features via a self-join, and writes the results to the
fan_in_features_fg feature group.

Detects "fan-in" structuring patterns: multiple distinct senders sending
near-threshold amounts ($8,000-$9,999) to the SAME receiver within a
24-hour window - a classic indicator of "smurfing" to stay below the
$10,000 BSA/CTR reporting limit.

Three-stage SQL:
  1. near_threshold       - filter & bucket transfers into 24-hour windows
  2. fan_in_self_join     - self-join to isolate fan-in patterns
  3. fan_in_features_fg   - aggregate per receiver per day

Requires:
  - money_transfers feature group (run transfers-datamart first)
  - Feldera Docker container (started automatically via pre-task)

Usage:
    python ccfraud/2d-fan-in-feldera-pipeline.py
    python ccfraud/2d-fan-in-feldera-pipeline.py --sleep-secs 180
"""

import json
import time
import subprocess
import argparse

import hopsworks
from hsfs.feature import Feature
from feldera import FelderaClient, PipelineBuilder


# ---------------------------------------------------------------------------
# CLI arguments
# ---------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(
        description="Feldera streaming pipeline for fan-in detection"
    )
    parser.add_argument("--feldera-url", type=str,
                        default="http://localhost:8080",
                        help="Feldera API URL (default: http://localhost:8080)")
    parser.add_argument("--sleep-secs", type=int, default=120,
                        help="Seconds to wait for Feldera before materializing")
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Kafka connector helpers (same pattern as 2b-ndi-feldera-pipeline.py)
# ---------------------------------------------------------------------------
def create_consumer_kafka_config(kafka_config: dict, fg):
    return kafka_config | {
        "topic": fg._online_topic_name,
        "start_from": "earliest",
    }


def create_producer_kafka_config(kafka_config: dict, fg, project):
    return kafka_config | {
        "topic": f"{project.name}_onlinefs",
        "auto.offset.reset": "earliest",
        "headers": [
            {"key": "projectId", "value": str(project.id)},
            {"key": "featureGroupId", "value": str(fg.id)},
            {"key": "subjectId", "value": str(fg.subject["id"])},
        ],
    }


# ---------------------------------------------------------------------------
# Feldera SQL - three-stage fan-in detection with self-join
# ---------------------------------------------------------------------------
def build_sql(source_config: str, sink_config: str) -> str:
    return f"""

    -- Source: money_transfers feature group (Kafka-backed)
    CREATE TABLE money_transfers (
        transfer_id      BIGINT,
        sender_id        VARCHAR,
        receiver_id      VARCHAR,
        amount           DOUBLE,
        sender_bank_id   VARCHAR,
        receiver_bank_id VARCHAR,
        currency         VARCHAR,
        event_time       TIMESTAMP
    ) WITH (
        'connectors' = '[{source_config}]'
    );

    -- Stage 1: Filter transfers near the $10,000 BSA/CTR reporting
    -- threshold and assign 24-hour day buckets.
    CREATE MATERIALIZED VIEW near_threshold AS
    SELECT
        transfer_id,
        sender_id,
        receiver_id,
        amount,
        event_time,
        FLOOR(UNIX_TIMESTAMP(event_time) / 86400) AS day_bucket
    FROM money_transfers
    WHERE amount >= 8000.0 AND amount < 10000.0;

    -- Stage 2: Self-join to isolate fan-in patterns.
    -- A near-threshold transfer is kept only when at least one OTHER
    -- near-threshold transfer from a DIFFERENT sender was sent to the
    -- SAME receiver in the SAME 24-hour window.
    CREATE VIEW fan_in_self_join AS
    SELECT DISTINCT
        a.receiver_id,
        a.day_bucket,
        a.sender_id,
        a.amount,
        a.event_time
    FROM near_threshold a
    JOIN near_threshold b
        ON  a.receiver_id = b.receiver_id
        AND a.day_bucket  = b.day_bucket
        AND a.sender_id  <> b.sender_id;

    -- Stage 3: Aggregate fan-in metrics per receiver per 24h window.
    CREATE VIEW fan_in_features_fg
    WITH (
        'connectors' = '[{sink_config}]'
    )
    AS
    SELECT
        receiver_id,
        day_bucket,
        MAX(event_time)            AS event_time,
        COUNT(DISTINCT sender_id)  AS fan_in_sender_count,
        COUNT(*)                   AS fan_in_transfer_count,
        SUM(amount)                AS fan_in_total_amount,
        AVG(amount)                AS fan_in_avg_amount,
        MIN(amount)                AS fan_in_min_amount,
        MAX(amount)                AS fan_in_max_amount
    FROM fan_in_self_join
    GROUP BY receiver_id, day_bucket;
    """


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    args = parse_args()

    # ------------------------------------------------------------------
    # Step 1: Hopsworks setup
    # ------------------------------------------------------------------
    client = FelderaClient(args.feldera_url)

    print("Connecting to Hopsworks...")
    project = hopsworks.login()
    hostname = project.get_url().removeprefix("https://").split(":", 1)[0]

    kafka_api = project.get_kafka_api()
    fs = project.get_feature_store()

    # Source feature group (must already exist - run transfers-datamart first)
    money_transfers_fg = fs.get_feature_group("money_transfers", version=1)

    # Sink feature group
    fan_in_fg = fs.get_or_create_feature_group(
        name="fan_in_features_fg",
        version=1,
        description=(
            "Fan-in detection features per receiver per 24-hour window. "
            "Counts distinct senders sending near-threshold amounts "
            "($8K-$10K) to the same receiver - potential structuring."
        ),
        primary_key=["receiver_id"],
        event_time="event_time",
        online_enabled=True,
        stream=True,
        features=[
            Feature("receiver_id", type="string"),
            Feature("day_bucket", type="bigint"),
            Feature("event_time", type="timestamp"),
            Feature("fan_in_sender_count", type="bigint"),
            Feature("fan_in_transfer_count", type="bigint"),
            Feature("fan_in_total_amount", type="double"),
            Feature("fan_in_avg_amount", type="double"),
            Feature("fan_in_min_amount", type="double"),
            Feature("fan_in_max_amount", type="double"),
        ],
    )

    try:
        fan_in_fg.save()
    except Exception:
        print("  Feature group fan_in_features_fg already exists")

    # ------------------------------------------------------------------
    # Step 2: Load certs into Feldera container
    # ------------------------------------------------------------------
    container_id = subprocess.check_output(
        ["docker", "ps", "--filter",
         "ancestor=ghcr.io/feldera/pipeline-manager:latest", "-q"],
        text=True,
    ).strip()
    print(f"\nFeldera container: {container_id}")

    subprocess.run([
        "docker", "exec", container_id,
        "bash", "-c",
        f"rm -f /tmp/{hostname} && "
        f"ln -s /opt/{hostname}/{hostname} /tmp/{hostname}",
    ])

    # ------------------------------------------------------------------
    # Step 3: Build SQL program
    # ------------------------------------------------------------------
    kafka_config = kafka_api.get_default_config()

    source_config = json.dumps({
        "transport": {
            "name": "kafka_input",
            "config": create_consumer_kafka_config(
                kafka_config, money_transfers_fg
            ),
        },
        "format": {
            "name": "avro",
            "config": {
                "schema": money_transfers_fg.avro_schema,
                "skip_schema_id": True,
            },
        },
    })

    sink_config = json.dumps({
        "transport": {
            "name": "kafka_output",
            "config": create_producer_kafka_config(
                kafka_config, fan_in_fg, project
            ),
        },
        "format": {
            "name": "avro",
            "config": {
                "schema": fan_in_fg.avro_schema,
                "skip_schema_id": True,
            },
        },
    })

    sql = build_sql(source_config, sink_config)
    pipeline = PipelineBuilder(
        client, name="hopsworks_fan_in_kafka", sql=sql
    ).create_or_replace()

    # ------------------------------------------------------------------
    # Step 4: Start pipeline and schedule materialization
    # ------------------------------------------------------------------
    print("\nStarting Feldera pipeline ...")
    pipeline.start()

    print(f"Sleeping {args.sleep_secs}s for Feldera to compute fan-in "
          "features before materializing to offline store ...")
    time.sleep(args.sleep_secs)

    fan_in_fg.materialization_job.run()
    print("\nFan-in Feldera pipeline completed successfully!")


if __name__ == "__main__":
    main()
