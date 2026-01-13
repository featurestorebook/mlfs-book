# Air Quality Prediction Service

This project builds a batch air quality forecasting service that predicts PM 2.5 (particulate matter) levels for the next 7 days. The system ingests data from public air quality sensors available at https://waqi.info/ and combines it with weather forecast data to generate accurate predictions.

## Overview

The air quality prediction service demonstrates a complete batch ML system with the following components:

- **Data Sources:** Public air quality sensors (PM 2.5 measurements) and weather APIs
- **Model:** XGBoost regression model trained on historical air quality and weather data
- **Features:** Historical PM 2.5 values, temperature, humidity, wind speed, pressure, and weather forecasts
- **Predictions:** 7-day air quality forecast updated regularly
- **Output:** Interactive dashboard displaying forecast with visualizations

## Architecture

The system follows a feature store-based architecture:
1. **Backfill Pipeline:** Loads historical air quality and weather data
2. **Feature Pipeline:** Computes and stores features in the feature store
3. **Training Pipeline:** Trains XGBoost model using historical features
4. **Inference Pipeline:** Generates 7-day forecasts using weather forecast data
5. **Dashboard:** Displays predictions and historical trends

## Prerequisites

- Python 3.10+
- Access to Hopsworks feature store (free account at https://www.hopsworks.ai/)
- API keys for weather services (configured in .env file)

## How to Run

Follow these steps to run the complete air quality prediction pipeline:

### Quickstart

```bash
source setup-env.sh
```

This script will:
- Create and activate a Python virtual environment
- Install all required dependencies
- Load environment variables from your .env file

### Run all the FTI Pipelines
```bash
inv all
```


You can also run the pipelines individually:

## Backfill historical data

```bash
inv backfill
```

This command loads historical air quality measurements and weather data into the feature store.

### Compute features

```bash
inv features
```

This command runs the feature pipeline to compute additional features (aggregations, time-based features, etc.) and stores them in the feature store.

### Train the model

```bash
inv train
```

This command trains the XGBoost regression model using historical features and registers it in the model registry.

### Generate predictions

```bash
inv inference
```

This command runs the inference pipeline to generate 7-day air quality forecasts and updates the dashboard.

## Additional Features

### Personalized Air Quality with LLM (Optional)

The air quality service can be augmented with LLM capabilities, allowing users to ask natural language questions about air quality forecasts and historical trends. The LLM prompt is enhanced with:
- Your location
- Current date
- Predicted air quality from the ML model
- Historical air quality data from the feature store
- Personalized recommendations based on sensitive groups

## Project Structure

```
airquality/
├── features/           # Feature engineering pipeline
├── training/           # Model training scripts
├── inference/          # Inference pipeline
├── notebooks/          # Jupyter notebooks for exploration
├── tasks.py            # Invoke task definitions
├── setup-env.sh        # Environment setup script
└── README.md           # This file
```

## Monitoring and Updates

After the initial setup, you can schedule regular updates:
- Run `inv features` daily to update features with new data
- Run `inv inference` daily to generate fresh 7-day forecasts
- Retrain the model periodically with `inv train` to incorporate new patterns

## Troubleshooting

- Ensure your .env file contains valid API keys for weather services
- Check that your Hopsworks credentials are properly configured
- If backfill fails, verify network connectivity to data sources
