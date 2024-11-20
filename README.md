# ID2223 Lab1 - Air Quality Prediction Service for Helsinki, Kallio 2, Finland 
Group 25 - Mengmeng Yang， Zongrong Yang

## Lab Introduction
This is the project of Lab1 for the course **ID2223 Scalable Machine Learning and Deep Learning**. This project implements a complete machine learning pipeline for predicting air quality (PM2.5 levels) in Helsinki, Finland using weather data and **lagged air quality for the previous 1-3 days**.

Here is the **[dashboard](https://lemongooo.github.io/mlfs-book/air-quality/)** shows next 9 days predictions and the hindcast graph which compares daily predicted PM2.5 levels against actual data to evaluate the model's accuracy.

## System Architecture
- Feature Store: Hopsworks
- Model Training: XGBoost Regressor
- Scheduling: GitHub Actions/Modal

## Backfill Feature Pipeline
   - Use the  `1_air_quality_feature_backfill.ipynb` to develop a pipeline that downloads historical air quality data of Helsinki, Kallio 2, Finland from [AQICN](https://aqicn.org) and weather data from [Open-Meteo](https://open-meteo.com).
   - Register those data as two Feature Groups in Hopsworks to ensure data consistency and completeness for model training.

## Feature Pipeline
   - Employ the `2_air_quality_feature_pipeline.ipynb` to set up a daily pipeline with GitHub Actions that fetches daily weather and air quality data and the upcoming 7-10 day weather forecast. 
   - Update new data in the Hopsworks Feature Groups to keep the model input current.

## Training Pipeline
   - Utilize the `3_air_quality_training_pipeline.ipynb` to merge selected features (PM2.5 with its lagged features and all weather data) and create a Feature View in Hopsworks.
   - Split the data into training and testing sets, train an XGBoost regressor, and evaluate the model's performance using MSE and R² metrics.
   - Register the model along with its schema and metrics in the Hopsworks Model Registry for centralized management and deployment.


## Batch Inference Pipeline
   - Use the `4_air_quality_batch_inference.ipynb notebook` to load the trained XGBoost model from the Hopsworks Model Registry.
   - Predict PM2.5 levels for the next 7-10 days by using the latest weather data and create a feature group to store predictions in Hopsworks.
   - Implement a dashboard with charts that show the predicted PM2.5 levels compared to the actual measurements.






## Lagged Feature
Air quality data typically shows time series correlations, meaning that today's air quality can influence the air quality in the following days. To enhance the model for predicting air quality, lagged features of PM2.5 are used. 
### Backfill
In the backfill phase, historical PM2.5 data is processed to create three lagged features, each representing air quality readings from one, two, and three days prior.
```python
df_aq['pm25'] = df_aq['pm25'].astype('float32')
df_aq['pm25_lag_1'] = df['pm25'].shift(1).astype('float32') # previous day
df_aq['pm25_lag_2'] = df['pm25'].shift(2).astype('float32') # two days ago
df_aq['pm25_lag_3'] = df['pm25'].shift(3).astype('float32') # three days ago
```
### Daily Updates
The system retrieves today's PM2.5 from the AQI API and fills them with lagged features (previous 1-3 days of PM2.5 values) from historical data. These combined features are then stored together in the air quality feature group for model usage.

### Training
The training set incorporates both PM2.5 lagged values and weather data.
```python
selected_features = air_quality_fg.select(['pm25','pm25_lag_1','pm25_lag_2','pm25_lag_3']).join(weather_fg.select_all())
```
This feature combination allows the model to capture both temporal and meteorological influences on air quality.

### Batch Inference
The batch inference process generates predictions step by step using weather data and historical values. For each new prediction, the model uses it as lagged input to forecast the next timestamp. However, error propagation is a drawback for this approach. Each prediction becomes a lagged feature for the next prediction, so any prediction errors will cascade and amplify through the sequence, potentially leading to decreased accuracy in longer-term forecasts.


### Result
By including these lagged features, the model can capture such time dependencies and improve prediction accuracy.

| | Without Lagged Features | With Lagged Features |
|---|---|---|
| **Model Performance** | ![image](without_lagged.png)| ![image](with_lagged.png) |

R-squared shifted from negative to positive values, demonstrating a significant improvement in model performance. MSE values decreased substantially, indicating higher prediction accuracy