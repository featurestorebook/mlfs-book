# Titanic Survival Prediction ML System

This project builds a Titanic survival prediction service that demonstrates the fundamentals of a batch ML system. It uses the classic Titanic dataset for training and generates synthetic passenger data for daily predictions. This starter project provides a simple, well-structured example of a complete ML pipeline with both batch predictions and interactive UI.

## Overview

The Titanic survival prediction service demonstrates a batch ML system with the following components:

- **Data Source:** Historical Titanic passenger data + synthetic passenger generator
- **Model:** XGBoost binary classifier (default hyperparameters)
- **Features:** Passenger demographics (age, sex, class), ticket information, family size
- **Predictions:** Survival probability (survived/perished) for synthetic passengers
- **Output:** GitHub Pages dashboard and interactive Gradio UI

## Two ML Interfaces

This project includes two complementary ML systems:

### 1. Batch Dashboard (Automated)
A batch ML system that runs daily to predict survival for synthetically generated passengers. Results are displayed on a GitHub Pages dashboard showing:
- Daily passenger profile
- Survival prediction
- Feature importance
- Historical trends

### 2. Interactive UI (On-Demand)
An interactive Gradio interface where you can:
- Enter custom passenger details (age, sex, class, etc.)
- Get instant survival predictions
- Explore how different features affect predictions
- Experiment with "what-if" scenarios

## Architecture

The system follows a feature store-based architecture:
1. **Backfill Pipeline:** Loads historical Titanic passenger data
2. **Feature Pipeline:** Engineers features (family size, fare bins, etc.)
3. **Training Pipeline:** Trains XGBoost classifier on historical data
4. **Inference Pipeline:** Generates predictions for synthetic passengers
5. **Dashboards:** Displays results via web UI and interactive tool

## Key Features

- **Simple & Educational:** Perfect starter project for learning ML pipelines
- **Classic Dataset:** Well-understood problem with interpretable results
- **Feature Engineering:** Demonstrates common feature transformations
- **Dual Interface:** Both automated batch and interactive predictions
- **Complete Pipeline:** Full end-to-end ML system example

## Prerequisites

- Python 3.10+
- Access to Hopsworks feature store (free account at https://www.hopsworks.ai/)
- GitHub account (optional, for deploying the dashboard)

## How to Run

Follow these steps to run the complete Titanic prediction pipeline:

### 1. Set up the environment

```bash
source setup-env.sh
```

This script will:
- Create and activate a Python virtual environment
- Install all required dependencies (scikit-learn, xgboost, gradio, etc.)
- Load environment variables from your .env file

### 2. Backfill historical data

```bash
inv backfill
```

This command loads the historical Titanic passenger dataset (training data with survival labels) into the feature store.

### 3. Compute features

```bash
inv features
```

This command runs the feature pipeline to engineer additional features:
- Family size (SibSp + Parch + 1)
- Title extraction from names (Mr., Mrs., Miss, etc.)
- Fare bins and ticket class interactions
- Age imputation and binning

These features are computed and stored in the feature store.

### 4. Train the model

```bash
inv train
```

This command trains the XGBoost binary classifier using the engineered features and survival labels, then registers the model in the model registry with performance metrics.

### 5. Generate predictions

```bash
inv inference
```

This command runs the inference pipeline to:
- Generate a synthetic passenger profile
- Retrieve features from the feature store
- Predict survival probability
- Update the batch dashboard with results

## Using the Interactive UI

To launch the Gradio interface for interactive predictions:

```bash
inv gradio
```

This opens a web interface where you can input passenger details and get instant predictions.

## Project Structure

```
titanic/
├── features/           # Feature engineering pipeline
├── training/           # Model training scripts
├── inference/          # Batch inference pipeline
├── notebooks/          # Jupyter notebooks for exploration
├── dashboard/          # Web dashboard code
├── tasks.py            # Invoke task definitions
├── setup-env.sh        # Environment setup script
└── README.md           # This file
```

## Understanding the Dataset

The Titanic dataset includes:
- **Passenger Info:** Name, age, sex
- **Ticket Info:** Class (1st, 2nd, 3rd), fare, cabin
- **Family Info:** Number of siblings/spouses, parents/children aboard
- **Outcome:** Survived (1) or perished (0)

Key patterns the model learns:
- Women and children had higher survival rates
- First-class passengers had better survival rates
- Family size affected survival chances

## Extending the Project

This starter project can be extended with:
- Additional features (deck location, embarkation port analysis)
- Hyperparameter tuning for improved accuracy
- Model comparison (try Random Forest, Logistic Regression)
- Monitoring and model drift detection
- Deployment as a REST API

## Troubleshooting

- Verify your Hopsworks credentials are properly configured in .env
- Ensure all dependencies are installed correctly (check setup-env.sh output)
- If feature engineering fails, check for missing values in the data
- For Gradio issues, ensure port 7860 is available
