# ID2223 Lab1 - Air Quality Prediction Service for Helsinki, Kallio 2, Finland 

## Lab Description
This project implements a complete machine learning pipeline for predicting air quality (PM2.5 levels) using weather data.
## Backfill Feature Pipeline
   - Use the  1_air_quality_feature_backfill.ipynb to develop a pipeline that downloads historical air quality data of Helsinki, Kallio 2, Finland from [AQICN](https://aqicn.org) and weather data from [Open-Meteo](https://open-meteo.com).
   - Register those data as two Feature Groups in Hopsworks to ensure data consistency and completeness for model training.

## Feature Pipeline
   - Daily feature updates using GitHub Actions
   - Feature storage and management using Hopsworks Feature Store

## Training Pipeline
   - Model training on historical weather and air quality data
   - Model registry and versioning with Hopsworks

## Batch Inference Pipeline
   - Daily predictions for the next 7 days
   - Automated updates via GitHub Actions/Modal

## Dashboard
Shows 7-day predictions and model performance metrics

[Dashboards for Example ML Systems](https://lemongooo.github.io/mlfs-book/air-quality/)

## Lagged Feature

