# Air Quality Analysis Project

This project, Lab 1 from KTH's ID2223 course in Scalable Machine Learning and Deep Learning, is inspired by the O'Reilly book "Building Machine Learning Systems with a Feature Store: Batch, Real-Time, and LLMs". It aims to analyze air quality data from a given location and provide insights using machine learning models. The project includes data retrieval, feature engineering, model training, and batch inference.

## Setup

1. Clone the repository:
    ```sh
    git clone https://github.com/yourusername/air-quality-analysis.git
    cd air-quality-analysis/notebooks/ch03/
    ```

2. Install the required dependencies:
    ```sh
    python3 -m venv .venv && \
    source .venv/bin/activate && \
    pip install -r requirements.txt
    ```

3. Set up your Hopsworks API key:
    - Save your Hopsworks API key in `data/hopsworks-api-key.txt`.

4. Set up your Aqi API Key:
    - Save your Aqi API key in `data/aqi-api-key.txt`.

## Usage

### Quality Feature Backfill

Create feature groups and backfill them with historical air quality data running the notebook `1_air_quality_feature_backfill.ipynb` in `notebooks/ch03/functions/1_air_quality_feature_backfill.ipynb`.

### Quality Feature Pipeline

The notebook `2_air_quality_feature_pipeline.ipynb` retrieves daily updates and backfills the two feature groups previously created. The feature pipeline will be scheduled to run once per day.

### Quality Training Pipeline

Train the air quality prediction model using the notebook `3_air_quality_training_pipeline.ipynb`.

### Batch Inference

Perform batch inference using the notebook `4_air_quality_batch_inference.ipynb`.

## License

This project is licensed under the Apache License 2.0. See the `LICENSE` file for details.