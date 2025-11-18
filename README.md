## Lab 1 -  Air Quality Prediction System
### Multi-Station Forecasting with Feature Stores, Lag Features, and Automated Pipelines

Built for KTH ID2223 – Scalable Machine Learning & Deep Learning
Inspired by the O’Reilly book Building Machine Learning Systems with a Feature Store (MLFS)

This repository contains an extended implementation of the air-quality prediction system from the MLFS book.
Our solution goes significantly beyond the single-sensor example by supporting:

- All air-quality stations in a selected city (Vienna)
- Automated ingestion of historical and daily sensor + weather data
- Lag features (t-1, t-2, t-3) computed per station
- Parametrized pipelines
- Daily batch inference for every station
- A dashboard with a map and interactive station-level forecasts. See the dashboard here: [Vienna Air Quality Dashboard](https://loretapajaziti.github.io/mlfs-book/air-quality/)



# Environment Setup

    # Create a conda or virtual environment for the project
    conda create -n book 
    conda activate book

    # Install 'uv' and 'invoke'
    pip install invoke dotenv

    # 'invoke install' installs python dependencies using uv and requirements.txt
    invoke install


## System Overview

Our system follows the architecture described in MLFS:

### 1. Backfill pipeline

- Lists all stations in the selected city (Vienna) with historial PM2.5
- Stores historical data of all stations into a Hopsworks Feature Group
- Fetches historical weather data

- Inserts into feature groups (air_quality_daily, weather_daily)


### 2. Daily feature pipeline

- Fetches yesterday’s PM2.5
- Fetches yesterday’s weather
- Fetches weather forecast
- Runs for all stations via GitHub Actions

### 3. Training pipeline

- Builds a Feature View
- Adds lag features (t-1, t-2, t-3)
- Trains an XGBoost regression model
- Registers the model in Hopsworks

### 4. Batch inference pipeline

- Runs daily for every station
- Loads each model
- Generates 7–10 day PM2.5 forecasts
- Produces PNG forecast plots
- Updates the GitHub Pages dashboard

### 5. Dashboard

- Shows a map of Vienna with station pins
- Clicking a station displays its forecast
- Updated automatically with every inference run


## Multi-Station Setup (What We Added)
The original tutorial supports only one sensor.
Our extension:

- Collects all AQICN stations in Vienna with PM2.5 data
- Creates a parameter dictionary for each station:
    - city/country/street 
    - AQICN API URL 
    - CSV URL 
    - unique secret name: SENSOR_LOCATION_JSON_<uid>
- Pipelines loop through this list to run daily updates, and inference for all sensors.

## CI/CD – GitHub Actions
The automation:
- runs the daily feature pipeline for aqi and weather
- runs batch inference
- updates dashboard PNGs
- publishes to GitHub Pages
