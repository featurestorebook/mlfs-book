# Real-Time Credit Card Fraud Detection

This project builds a real-time credit card fraud detection service that processes transactions as they occur and predicts the probability of fraud in real-time. The system uses streaming feature engineering with Feldera and machine learning to identify suspicious transactions with low latency.

## Overview

The credit card fraud detection service demonstrates a complete real-time ML system with the following components:

- **Data Source:** Synthetic credit card transaction data simulating realistic patterns
- **Stream Processing:** Feldera for real-time feature computation
- **Model:** XGBoost binary classifier trained on transaction patterns and aggregate features
- **Features:** Transaction amount, location, velocity features, aggregate statistics
- **Predictions:** Real-time fraud probability (0-1) for each transaction
- **Output:** Low-latency fraud detection service

## Architecture

The system follows a real-time feature store-based architecture:
1. **Backfill Pipeline:** Loads historical transaction data for training
2. **Stream Processing (Feldera):** Computes real-time features from transaction streams
3. **Feature Pipeline:** Creates and stores features in the feature store
4. **Training Pipeline:** Trains XGBoost classifier on historical fraud patterns


## Key Features

- **Real-Time Processing:** Sub-second latency for fraud detection
- **Velocity Features:** Transaction frequency and spending patterns over time windows
- **Aggregate Features:** Historical spending patterns by merchant, location, and category
- **Streaming Architecture:** Feldera processes transactions as they arrive
- **Synthetic Data:** Realistic transaction patterns with labeled fraud examples

## Prerequisites

- Python 3.12+
- Docker (for running Feldera stream processor)
- Access to Hopsworks feature store (free account at https://www.hopsworks.ai/)

## How to Run

Follow these steps to run the complete real-time fraud detection pipeline:

### 1. Set up the environment

```bash
source setup-env.sh
```

This script will:
- Create and activate a Python virtual environment
- Install all required dependencies
- Load environment variables from your .env file

### 2. Backfill historical data

```bash
inv backfill
```

This command generates and loads synthetic historical transaction data (including fraud labels) into the feature store.

### 3. Start Feldera stream processor

```bash
inv feldera-start
```

This command starts Feldera in a Docker container. Feldera will run in the foreground and process streaming transactions. Note: Keep this terminal open or run in a separate session.

### 4. Configure Feldera pipelines

In a new terminal (with the environment activated), run:

```bash
inv feldera
```

This command configures the Feldera streaming pipelines for real-time feature computation.

### 5. Compute features

```bash
inv features
```

This command runs the feature pipeline to compute aggregate and velocity features from historical data and stores them in the feature store.

### 6. Train the model

```bash
inv train
```

This command trains the XGBoost binary classifier using historical features and fraud labels, then registers the model in the model registry.

## TODO: Real-Time Inference



Once the system is running, new transactions are processed in real-time:
1. Transaction arrives in the stream
2. Feldera computes velocity and aggregate features
3. Features are retrieved from the feature store
4. Model predicts fraud probability
5. High-risk transactions are flagged for review

## Project Structure

```
ccfraud/
├── features/           # Feature engineering pipeline
├── training/           # Model training scripts
├── inference/          # Real-time inference service
├── notebooks/          # Jupyter notebooks for exploration
├── feldera/            # Feldera streaming pipeline configs
├── tasks.py            # Invoke task definitions
├── setup-env.sh        # Environment setup script
└── README.md           # This file
```

## Monitoring and Operations

- Monitor Feldera logs for stream processing metrics
- Track model performance on fraud detection rate and false positives
- Retrain the model periodically with `inv train` as new fraud patterns emerge
- Scale Feldera for higher transaction throughput as needed

## Understanding the Synthetic Data

The synthetic transaction data includes:
- Normal spending patterns (majority of transactions)
- Fraud patterns: unusual amounts, rapid transactions, geographic anomalies
- Labels: 0 (legitimate) and 1 (fraud)
- Realistic distributions of merchants, locations, and transaction amounts

## Troubleshooting

- If Feldera fails to start, ensure Docker is running and port 8080 is available
- Check Feldera logs for stream processing errors
- Verify your Hopsworks credentials are properly configured in .env
- Ensure sufficient memory for both Feldera and the ML model