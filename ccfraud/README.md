# Real-Time Credit Card Fraud Detection

This project builds a real-time credit card fraud detection service that processes transactions as they occur and predicts the probability of fraud in real-time. The system uses streaming feature engineering with Spark Structured Streaming on Hopsworks and machine learning to identify suspicious transactions with low latency.

## Overview

The credit card fraud detection service demonstrates a complete real-time ML system with the following components:

- **Data Source:** Synthetic credit card transaction data simulating realistic patterns
- **Stream Processing:** Spark Structured Streaming job on Hopsworks, computing per-card aggregates over a sliding window
- **Model:** XGBoost binary classifier trained on transaction patterns and aggregate features
- **Features:** Transaction amount, location, velocity features, aggregate statistics
- **Predictions:** Real-time fraud probability (0-1) for each transaction
- **Output:** Low-latency fraud detection service

## Architecture

The system follows a real-time feature store-based architecture:
1. **Backfill Pipeline:** Loads historical transaction data for training (`1_data_generator.py`)
2. **Transaction Generator (Hopsworks Python job):** Writes a live stream of transactions, 100 per minute by default, to the `credit_card_transactions` feature group (`1b-transaction-generator-job.py`)
3. **Streaming Feature Pipeline (Hopsworks PySpark job):** Reads the transactions' Kafka topic and computes per-card aggregates over a 1-hour window that slides every minute, written to `cc_trans_aggs_fg` v2 (`2-spark-streaming-feature-pipeline.py`). This is the only Spark job of the example.
4. **Aggregates Backfill (Hopsworks Python job, Polars):** Computes the same windows over the transaction history for training (`2b-backfill-aggs-pipeline.py`). For every transaction it writes the window a point-in-time join selects, the card's latest window that ended before the transaction, so the offline table holds at most one row per transaction instead of the ~60 windows every transaction falls into; each card's latest window goes to the online store.
5. **Batch Feature Pipeline (Hopsworks Python job, Polars):** Creates and stores per-transaction features in the feature store (`3-batch-feature-pipeline.py`)
6. **Training Pipeline:** Trains XGBoost classifier on historical fraud patterns

All jobs are Python jobs computing with Polars, except the streaming job. Every feature group is created with `statistics_config=False`: with statistics enabled, each insert from a Python job launches a PySpark `<feature group>_<version>_compute_stats` job, which for the transaction generator meant one Spark job per minute.

### Sliding-window features

Instead of rolling (per-row `OVER`) aggregations, the streaming pipeline uses a hopping event-time window: 60 minutes long, sliding every minute. Spark keeps one state row per (card, window) and drops it once the watermark passes the window's end, so state stays bounded and the job scales out by `cc_num`. The 10-minute features are conditional aggregates inside the same window, so they cost no extra windows.

| Feature | Meaning (relative to `event_time`, the window end) |
|---|---|
| `num_trans_last_10_mins`, `sum_trans_last_10_mins` | count / total amount in the last 10 minutes |
| `num_trans_last_hour`, `sum_trans_last_hour`, `max_trans_last_hour` | count / total / largest amount in the last hour |
| `num_ip_addresses_last_hour` | approximate distinct IP addresses in the last hour |
| `prev_ts`, `prev_ip_address`, `prev_card_present` | the card's last transaction before `event_time` |

A window is written when the watermark (default 2 minutes) passes its end, so online features are at most ~3 minutes old. A window with no transactions emits no row, so a card's online row keeps its last non-empty window. Day/week aggregates are not computed: with a 1-minute slide they would put 1,440-10,080 copies of every transaction in state; compute them in a batch pipeline.


## Key Features

- **Real-Time Processing:** Sub-second latency for fraud detection
- **Velocity Features:** Transaction frequency and spending patterns over time windows
- **Aggregate Features:** Historical spending patterns by merchant, location, and category
- **Streaming Architecture:** Spark Structured Streaming processes transactions as they arrive
- **Synthetic Data:** Realistic transaction patterns with labeled fraud examples

## Prerequisites

- Python 3.12+
- Access to Hopsworks feature store (free account at https://www.hopsworks.ai/)

## How to Run

Follow these steps to run the complete real-time fraud detection pipeline:

### 1. Set up the environment

```bash
source setup.sh
```

This script will:
- Create and activate a Python virtual environment
- Install all required dependencies
- Load environment variables from your .env file

### 2. Create and backfill our data mart with synthetic data

```bash
inv datamart
```

This command generates and loads synthetic historical transaction data (including fraud labels) into the feature store.

### 3. Run the streaming jobs in Hopsworks

```bash
inv backfill-aggs          # Polars job: sliding-window aggregates over the transaction history
inv streaming-features     # Spark Structured Streaming job (runs until stopped)
inv stream-transactions    # Python job: 100 transactions/minute (runs until stopped)
inv stream-transactions --transactions-per-min=1000   # a different rate
inv stream-status          # state of the jobs
inv stop-streams           # stop the generator and the streaming job
```

These create Hopsworks jobs (`ccfraud-backfill-aggs`, `ccfraud-streaming-aggs`, `ccfraud-transactions`) with `ccfraud/jobs.py`. Run the backfill before the streaming job: it creates the `cc_trans_aggs_fg` feature group, whose schema lives in `ccfraud/features/cc_trans_aggs_fg.py`. Inside Hopsworks the jobs run the repo's scripts in place from HopsFS; from a laptop the scripts are uploaded to `Resources/mlfs-book` first. The streaming job checkpoints to `Resources/ccfraud/checkpoints`, so a restart resumes where it stopped, and it schedules the hourly offline materialization of `cc_trans_aggs_fg`.

### 4. Compute features

```bash
inv features
```

This command runs the feature pipeline to compute aggregate and velocity features from historical data and stores them in the feature store.

### 5. Train the model

```bash
inv train
```
This command trains the XGBoost binary classifier using historical features and fraud labels, then registers the model in the model registry, and deploys it to Hopsowrks for serving.

### 6. Inference with the ML systems

```bash
inv inference
```
This command  starts a local streamlit application that invokes the model deployed on Hopsworks.



Once the system is running, new transactions are processed in real-time:
1. Transaction arrives in the stream
2. The Spark streaming job updates the card's sliding-window aggregates every minute
3. Features are retrieved from the feature store
4. Model predicts fraud probability
5. High-risk transactions are flagged for review

## Monitoring and Operations

### Feature monitoring: drift of the transaction amount

```bash
inv monitoring             # create the hourly monitoring job (idempotent)
inv monitoring --run-now   # ... and run it once right away
inv monitoring --replace   # recreate it after changing the parameters in ccfraud/5-feature-monitoring.py
```

`ccfraud/5-feature-monitoring.py` attaches a feature monitoring configuration (`amount_psi_hourly`) to the
`credit_card_transactions` feature group. Hopsworks runs it as the job
`credit_card_transactions_1_run_fm_amount_psi_hourly` at the top of every hour: it computes the distribution of
`amount` over the last day of transactions (by event time `ts`) and over the week before that, and compares
them with the Population Stability Index (PSI). A PSI of 0.2 or more marks the run as a detected shift, shown
on the feature group's monitoring tab, and an alert on `feature_monitor_shift_detected` can notify a receiver.

PSI monitoring needs the feature group's statistics configuration to carry the `kll` flag (the KLL sketch
the distribution is estimated with); the script sets it. Statistics stay disabled (`enabled=False`), so inserts
still launch no statistics job: the monitoring job profiles the two windows itself when it runs.

### Operations

- Monitor the `ccfraud-streaming-aggs` job logs (Spark UI) for stream processing metrics
- Track model performance on fraud detection rate and false positives
- Retrain the model periodically with `inv train` as new fraud patterns emerge
- Scale the streaming job's executors for higher transaction throughput as needed

## Understanding the Synthetic Data

The synthetic transaction data includes:
- Normal spending patterns (majority of transactions)
- Fraud patterns: unusual amounts, rapid transactions, geographic anomalies
- Labels: 0 (legitimate) and 1 (fraud)
- Realistic distributions of merchants, locations, and transaction amounts

## Troubleshooting

- If no aggregates appear, check `inv stream-status` and the `ccfraud-streaming-aggs` job logs
- Verify your Hopsworks credentials are properly configured in .env
- Ensure the streaming job has enough executor memory for its window state
