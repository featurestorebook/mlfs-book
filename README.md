# Machine Learning Feature Store Book - Example Projects

This repository contains three complete machine learning systems demonstrating different architectural patterns and use cases. Each project showcases best practices for building production ML systems with feature stores.

## Projects

### 1. Batch Air Quality Predictions

A batch ML system that forecasts air quality (PM 2.5) for the next 7 days using real-time weather data and historical air quality measurements from public sensors (https://waqi.info/). The system uses XGBoost to generate predictions and displays them on an interactive dashboard.

**Type:** Batch Predictions
**Model:** XGBoost Regression
**Features:** Weather forecasts, historical air quality data
**Output:** 7-day air quality forecast dashboard

![Air quality Prediction](https://featurestorebook.github.io/mlfs-book/air-quality/assets/img/pm25_forecast.png)

**Optional LLM Enhancement:** The air quality service can be augmented with LLM capabilities for natural language queries about air quality forecasts and historical trends.

[View Project →](./airquality)

### 2. Real-Time Credit Card Fraud Detection

A real-time ML system that detects fraudulent credit card transactions using streaming data and real-time feature engineering. This project demonstrates how to build low-latency prediction services that process transactions as they occur, using Feldera for stream processing.

**Type:** Real-Time Predictions
**Model:** Binary Classification (XGBoost)
**Features:** Real-time transaction aggregates, velocity features
**Output:** Fraud probability for each transaction
**Data:** Synthetic credit card transactions

[View Project →](./ccfraud)

### 3. Starter Titanic Batch Predictions

A starter ML system for predicting Titanic passenger survival. This project provides a simple, well-structured example of a batch ML pipeline using the classic Titanic dataset for training and synthetic data for generating daily predictions. Includes both a batch dashboard and an interactive UI.

**Type:** Batch Predictions
**Model:** Binary Classification (XGBoost)
**Features:** Passenger demographics, ticket class, family size
**Output:** Dashboard and interactive Gradio UI
**Data:** Historical Titanic dataset + synthetic passengers

[View Project →](./titanic)

## Architecture

Each project follows a similar architectural pattern:
- **Feature Store:** Central repository for feature storage and retrieval
- **Feature Pipeline:** ETL jobs that compute and store features
- **Training Pipeline:** Model training with versioning and experiment tracking
- **Inference Pipeline:** Batch or real-time predictions
- **UI/Dashboard:** Visualization of predictions and monitoring

![Application Architecture](app-air-quality-with-llms.png)

## Getting Started

Each project can be run independently. Navigate to the project directory and follow the instructions in its README.md file.

## Additional Resources

You can find [additional tutorial instructions in this Google Doc](https://docs.google.com/document/d/1YXfM1_rpo1-jM-lYyb1HpbV9EJPN6i1u6h2rhdPduNE/edit?usp=sharing).

