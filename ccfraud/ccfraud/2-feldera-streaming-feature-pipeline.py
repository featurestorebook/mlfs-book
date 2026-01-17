#!/usr/bin/env python
"""
Step 1: Real-Time Feature Computation

This script is part of a demo showcasing a real-time fraud detection pipeline,
utilizing Feldera for feature computation and Hopsworks as the feature store.

Real-time feature engineering pipeline using Feldera and Hopsworks
"""

import sys
from pathlib import Path
import hopsworks
import hsfs
from hsfs.feature import Feature
import json
import datetime
from feldera import FelderaClient, PipelineBuilder
import subprocess
import time

# Step 1.0: Setup paths and environment
root_dir = Path().absolute()
# Strip ~/notebooks/ccfraud from PYTHON_PATH if script started in one of these subdirectories
if root_dir.parts[-1:] == ('notebooks',):
    root_dir = Path(*root_dir.parts[:-1])
    sys.path.append(str(root_dir))
if root_dir.parts[-1:] == ('ccfraud',):
    root_dir = Path(*root_dir.parts[:-1])
    sys.path.append(str(root_dir))
root_dir = str(root_dir)

print(f"Root dir: {root_dir}")

# Set the environment variables from the file <root_dir>/.env
from mlfs import config
settings = config.HopsworksSettings(_env_file=f"{root_dir}/.env")

# Step 1.1. Create Hopsworks feature groups
client = FelderaClient("http://localhost:8080")

project = hopsworks.login()
hostname = project.get_url().removeprefix("https://").split(":", 1)[0]

kafka_api = project.get_kafka_api()

fs = project.get_feature_store()

name = "cc_trans_aggs_fg"
kafka_topic = f"{project.name}_onlinefs"
aggs_topic = f"{project.name}_{name}_onlinefs"

cc_trans_fg = fs.get_feature_group(name="credit_card_transactions", version=1)
card_details_fg = fs.get_feature_group(name="card_details", version=1)


# WINDOWED - frequency of transactions and other metrics in the span of a few hours,
# modeled as hopping window aggregates.
windowed_fg = fs.get_or_create_feature_group(
    name=name,
    primary_key=["cc_num"],
    online_enabled=True,
    version=1,
    event_time="event_time",
    stream=True,
    features=[
        Feature("t_id", type="bigint"),
        Feature("cc_num", type="string"),
        Feature("account_id", type="string"),
        Feature("bank_id", type="string"),
        Feature("num_trans_last_10_mins", type="bigint"),
        Feature("sum_trans_last_10_mins", type="double"),
        Feature("num_trans_last_hour", type="bigint"),
        Feature("sum_trans_last_hour", type="double"),
        Feature("num_trans_last_day", type="bigint"),
        Feature("sum_trans_last_day", type="double"),
        Feature("num_trans_last_week", type="bigint"),
        Feature("sum_trans_last_week", type="double"),
        Feature("prev_card_present", type="boolean"),
        Feature("prev_ip_transaction", type="string"),
        Feature("prev_ts_transaction", type="timestamp"),
        Feature("event_time", type="timestamp"),
    ],
)

try:
    windowed_fg.save()
except Exception as e:
    print("Feature group already exists")

# Load certs in Feldera Container
# Feldera expects the certs to be in /tmp/HOPSWORKS_HOST/HOPSWORKS_PROJECT

# Get container ID
container_id = subprocess.check_output(
    ["docker", "ps", "--filter", "ancestor=ghcr.io/feldera/pipeline-manager:latest", "-q"],
    text=True
).strip()

print(f"container_id is {container_id}")
# Run the command inside the container
subprocess.run([
    "docker", "exec", container_id,
    "bash", "-c",
    f"rm -f /tmp/{hostname} && ln -s /opt/{hostname}/{hostname} /tmp/{hostname}"
])

# Step 1.2. Create Feldera pipeline
# We build a Feldera pipeline to transform raw transaction and profile data into features.
# In Feldera, feature groups are modeled as SQL views. Thus, we create a SQL program with
# two input tables (TRANSACTIONS and PROFILES), and two output views, one for each feature group.


def build_sql(
    transaction_source_config: str, card_details_source_config: str, fs_sink_config: str
) -> str:
    """Create SQL program parameterized by source and sink connector configurations."""
    return f"""

    CREATE TABLE credit_card_transactions (
        t_id BIGINT,
        merchant_id VARCHAR,
        ts TIMESTAMP,
        cc_num VARCHAR,
        amount DOUBLE,
        ip_address VARCHAR,
        card_present BOOLEAN
    ) WITH (
        'connectors' = '[{transaction_source_config}]'
    );

    CREATE MATERIALIZED VIEW rolling_aggregates AS
    SELECT
        t.t_id,
        t.cc_num,
        t.ts AS event_time,
        t.ip_address,
        t.card_present,
        SUM(COALESCE(amount, 0)) OVER window_10_minute AS sum_trans_last_10_mins,
        COUNT(amount) OVER window_10_minute AS num_trans_last_10_mins,
        SUM(COALESCE(amount, 0)) OVER window_1_hour AS sum_trans_last_hour,
        COUNT(amount) OVER window_1_hour AS num_trans_last_hour,
        SUM(COALESCE(amount, 0)) OVER window_1_day AS sum_trans_last_day,
        COUNT(amount) OVER window_1_day AS num_trans_last_day,
        SUM(COALESCE(amount, 0)) OVER window_7_day AS sum_trans_last_week,
        COUNT(amount) OVER window_7_day AS num_trans_last_week
    FROM
         credit_card_transactions AS t
    WINDOW
        window_10_minute AS (
            PARTITION BY cc_num
            ORDER BY ts
            RANGE BETWEEN INTERVAL '10' MINUTE PRECEDING AND CURRENT ROW
        ),
        window_1_hour AS (
            PARTITION BY cc_num
            ORDER BY ts
            RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
        ),
        window_1_day AS (
            PARTITION BY cc_num
            ORDER BY ts
            RANGE BETWEEN INTERVAL '1' DAY PRECEDING AND CURRENT ROW
        ),
        window_7_day AS (
            PARTITION BY cc_num
            ORDER BY ts
            RANGE BETWEEN INTERVAL '7' DAY PRECEDING AND CURRENT ROW
        )
    ;

    CREATE TABLE card_details (
        card_id VARCHAR,
        cc_num VARCHAR NOT NULL,
        account_id VARCHAR NOT NULL,
        bank_id VARCHAR NOT NULL,
        cc_expiry_date TIMESTAMP,
        issue_date TIMESTAMP,
        card_type VARCHAR,
        status VARCHAR,
        last_modified TIMESTAMP
    ) WITH (
        'connectors' = '[{card_details_source_config}]'
    );


    CREATE MATERIALIZED VIEW cc_trans_card AS
    SELECT
        ra.*,
        cd.account_id,
        cd.bank_id
    FROM rolling_aggregates AS ra
    LEFT ASOF JOIN card_details AS cd
        MATCH_CONDITION (ra.event_time >= cd.last_modified)
        ON ra.cc_num = cd.cc_num
    ;

    CREATE MATERIALIZED VIEW lagged_trans AS
    SELECT
        ctc.*,
        LAG(event_time) OVER
          (PARTITION BY cc_num ORDER BY event_time ASC) AS prev_ts_transaction,
        LAG(ip_address) OVER
          (PARTITION BY cc_num ORDER BY ip_address ASC) AS prev_ip_transaction,
        LAG(card_present) OVER
          (PARTITION BY cc_num ORDER BY card_present ASC) AS prev_card_present
    FROM cc_trans_card AS ctc;

    CREATE VIEW cc_trans_aggs_fg
    WITH (
        'connectors' = '[{fs_sink_config}]'
    )
    AS
        SELECT
            t_id,
            cc_num,
            event_time,
            account_id,
            bank_id,
            sum_trans_last_10_mins,
            num_trans_last_10_mins,
            sum_trans_last_hour,
            num_trans_last_hour,
            sum_trans_last_day,
            num_trans_last_day,
            sum_trans_last_week,
            num_trans_last_week,
            prev_ts_transaction,
            prev_ip_transaction,
            prev_card_present
        FROM lagged_trans
    ;
    """


# Connect Kafka sources and sinks
# We use the Kafka topic created during the data prep step as the input for the TRANSACTIONS table.
# The output views are also connected to the Hopsworks feature store via Kafka. Hopsworks ingests
# data from Kafka using the Avro format, so we configure Feldera Kafka connectors with Avro schemas
# generated by Hopsworks for each feature group.

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
            {
                "key": "projectId",
                "value": str(project.id),
            },
            {
                "key": "featureGroupId",
                "value": str(fg.id),
            },
            {
                "key": "subjectId",
                "value": str(fg.subject["id"]),
            },
        ],
    }


kafka_config = kafka_api.get_default_config()

fs_sink_config = json.dumps(
    {
        "transport": {
            "name": "kafka_output",
            "config": create_producer_kafka_config(kafka_config, windowed_fg, project),
        },
        "format": {
            "name": "avro",
            "config": {"schema": windowed_fg.avro_schema, "skip_schema_id": True},
        },
    }
)

transaction_source_config = json.dumps(
    {
        "transport": {
            "name": "kafka_input",
            "config": create_consumer_kafka_config(kafka_config, cc_trans_fg),
        },
        "format": {
            "name": "avro",
            "config": {"schema": cc_trans_fg.avro_schema, "skip_schema_id": True},
        },
    }
)

card_details_source_config = json.dumps(
    {
        "transport": {
            "name": "kafka_input",
            "config": create_consumer_kafka_config(kafka_config, card_details_fg),
        },
        "format": {
            "name": "avro",
            "config": {"schema": card_details_fg.avro_schema, "skip_schema_id": True},
        },
    }
)

sql = build_sql(transaction_source_config, card_details_source_config, fs_sink_config)
pipeline = PipelineBuilder(client, name="hopsworks_kafka", sql=sql).create_or_replace()

# Step 1.3. Run the pipeline
# Start the Feldera pipeline.
# Read profile data from the feature store and write it to the `PROFILE` table.
pipeline.start()

# Schedule periodic materialization to the offline store
# Wait until data has been written before materializing to the offline store
sleep_secs = 60
print(f"Sleeping for {sleep_secs} secs for feldera to compute and write features before materializing to offline store")
time.sleep(sleep_secs)

windowed_fg.materialization_job.run()
