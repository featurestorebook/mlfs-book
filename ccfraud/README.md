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
3. **Streaming Feature Pipeline (Hopsworks PySpark job):** Reads the transactions' Kafka topic and computes per-card aggregates over a 1-hour window that slides every minute, written to `cc_trans_aggs_fg` v2 (`2-spark-streaming-feature-pipeline.py`). The same program in `--mode backfill` computes the windows for the transaction history.
4. **Batch Feature Pipeline:** Creates and stores per-transaction features in the feature store
5. **Training Pipeline:** Trains XGBoost classifier on historical fraud patterns

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
inv backfill-aggs          # Spark job: sliding-window aggregates over the transaction history
inv streaming-features     # Spark Structured Streaming job (runs until stopped)
inv stream-transactions    # Python job: 100 transactions/minute (runs until stopped)
inv stream-transactions --transactions-per-min=1000   # a different rate
inv stream-status          # state of the jobs
inv stop-streams           # stop the generator and the streaming job
```

These create Hopsworks jobs (`ccfraud-backfill-aggs`, `ccfraud-streaming-aggs`, `ccfraud-transactions`) with `ccfraud/jobs.py`. Inside Hopsworks the jobs run the repo's scripts in place from HopsFS; from a laptop the scripts are uploaded to `Resources/mlfs-book` first. The streaming job checkpoints to `Resources/ccfraud/checkpoints`, so a restart resumes where it stopped, and it schedules the hourly offline materialization of `cc_trans_aggs_fg`.

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
