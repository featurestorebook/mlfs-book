#!/usr/bin/env python3
"""
NDI (Net Disposable Income) Stability - Feldera Streaming Feature Pipeline

Reads from the account_ledger feature group (via Kafka), computes monthly
net income aggregations and rolling 12-month stability features, and writes
the results to the ndi_features_fg feature group.

Two-stage SQL:
  1. monthly_net  - bucket events into ~30-day months, aggregate per account
  2. ndi_features_fg - rolling 12-month window over monthly buckets
"""

import sys
from pathlib import Path
import json
import time
import subprocess

import hopsworks
from hsfs.feature import Feature
from feldera import FelderaClient, PipelineBuilder

# Setup paths
current_file = Path(__file__).absolute()
ccfraud_pkg_dir = current_file.parent
ccfraud_project_dir = ccfraud_pkg_dir.parent
root_dir = ccfraud_project_dir.parent

sys.path.insert(0, str(root_dir))
sys.path.insert(0, str(ccfraud_project_dir))

from mlfs import config

print(f"Root dir: {root_dir}")

settings = config.HopsworksSettings(_env_file=str(root_dir / ".env"))

# ---------------------------------------------------------------------------
# Step 1: Hopsworks setup
# ---------------------------------------------------------------------------
client = FelderaClient("http://localhost:8080")

project = hopsworks.login()
hostname = project.get_url().removeprefix("https://").split(":", 1)[0]

kafka_api = project.get_kafka_api()
fs = project.get_feature_store()

# Source feature group
account_ledger_fg = fs.get_feature_group("account_ledger", version=1)

# Sink feature group
ndi_fg = fs.get_or_create_feature_group(
    name="ndi_features_fg",
    primary_key=["account_id"],
    online_enabled=True,
    version=1,
    event_time="event_time",
    stream=True,
    features=[
        Feature("transaction_id", type="bigint"),
        Feature("account_id", type="string"),
        Feature("event_time", type="timestamp"),
        Feature("current_month_net", type="double"),
        Feature("total_net_12m", type="double"),
        Feature("avg_monthly_net_12m", type="double"),
        Feature("months_with_data", type="bigint"),
        Feature("max_monthly_net_12m", type="double"),
        Feature("min_monthly_net_12m", type="double"),
        Feature("ndi_range_12m", type="double"),
    ],
)

try:
    ndi_fg.save()
except Exception:
    print("Feature group ndi_features_fg already exists")

# ---------------------------------------------------------------------------
# Step 2: Load certs into Feldera container
# ---------------------------------------------------------------------------
container_id = subprocess.check_output(
    ["docker", "ps", "--filter", "ancestor=ghcr.io/feldera/pipeline-manager:latest", "-q"],
    text=True,
).strip()

print(f"container_id is {container_id}")

subprocess.run([
    "docker", "exec", container_id,
    "bash", "-c",
    f"rm -f /tmp/{hostname} && ln -s /opt/{hostname}/{hostname} /tmp/{hostname}",
])

# ---------------------------------------------------------------------------
# Step 3: Build SQL program
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


def build_sql(ledger_source_config: str, fs_sink_config: str) -> str:
    return f"""

    CREATE TABLE account_ledger (
        transaction_id BIGINT,
        account_id VARCHAR,
        credit DOUBLE,
        debit DOUBLE,
        event_time TIMESTAMP
    ) WITH (
        'connectors' = '[{ledger_source_config}]'
    );

    -- Stage 1: Monthly aggregation per account
    CREATE MATERIALIZED VIEW monthly_net AS
    SELECT
        account_id,
        FLOOR(UNIX_TIMESTAMP(event_time) / 2592000) AS month_bucket,
        MAX(event_time) AS event_time,
        MAX(transaction_id) AS transaction_id,
        SUM(credit) AS total_credits,
        SUM(debit) AS total_debits,
        SUM(credit) - SUM(debit) AS net_income
    FROM account_ledger
    GROUP BY account_id, FLOOR(UNIX_TIMESTAMP(event_time) / 2592000);

    -- Stage 2: Rolling 12-month window over monthly buckets
    CREATE VIEW ndi_features_fg
    WITH (
        'connectors' = '[{fs_sink_config}]'
    )
    AS
    SELECT
        account_id,
        transaction_id,
        event_time,
        net_income AS current_month_net,
        SUM(net_income) OVER w12 AS total_net_12m,
        AVG(net_income) OVER w12 AS avg_monthly_net_12m,
        COUNT(*) OVER w12 AS months_with_data,
        MAX(net_income) OVER w12 AS max_monthly_net_12m,
        MIN(net_income) OVER w12 AS min_monthly_net_12m,
        MAX(net_income) OVER w12 - MIN(net_income) OVER w12 AS ndi_range_12m
    FROM monthly_net
    WINDOW w12 AS (
        PARTITION BY account_id
        ORDER BY month_bucket
        ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    );
    """


# ---------------------------------------------------------------------------
# Step 4: Configure Kafka connectors
# ---------------------------------------------------------------------------
kafka_config = kafka_api.get_default_config()

fs_sink_config = json.dumps({
    "transport": {
        "name": "kafka_output",
        "config": create_producer_kafka_config(kafka_config, ndi_fg, project),
    },
    "format": {
        "name": "avro",
        "config": {"schema": ndi_fg.avro_schema, "skip_schema_id": True},
    },
})

ledger_source_config = json.dumps({
    "transport": {
        "name": "kafka_input",
        "config": create_consumer_kafka_config(kafka_config, account_ledger_fg),
    },
    "format": {
        "name": "avro",
        "config": {"schema": account_ledger_fg.avro_schema, "skip_schema_id": True},
    },
})

sql = build_sql(ledger_source_config, fs_sink_config)
pipeline = PipelineBuilder(client, name="hopsworks_ndi_kafka", sql=sql).create_or_replace()

# ---------------------------------------------------------------------------
# Step 5: Start pipeline and schedule materialization
# ---------------------------------------------------------------------------
pipeline.start()

sleep_secs = 120
print(f"Sleeping for {sleep_secs} secs for Feldera to compute and write features before materializing to offline store")
time.sleep(sleep_secs)

ndi_fg.materialization_job.run()
