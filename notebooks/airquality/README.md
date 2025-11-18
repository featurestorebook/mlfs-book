## Predict Air Quality

This project builds an Air Quality Forecasting Service for an Air Quality sensor available at https://waqi.info/.


The output is a forecast for the air quality in Visby, Sweden. An example for one of the sensors:

![Air quality Prediction](https://raw.githubusercontent.com/jpruzcuen/mlfs-book/refs/heads/main/docs/air-quality/assets/img/bromsebrovag_8/pm25_forecast.png)



## Personalized Air Quality Predictions with a LLM

This air quality forecasting service has been augmented with some features including lagged measurements of PM25 for 1, 2 and 3 days. These are good short term indicators of the air quality. In addition, rolling averages with windows 3, 7 and 14 days are also used as features to include the longer trends in the air quality.

In addition, 2 sensors for Visby are being automatically monitored, with forecasts and hindcasts run in a daily schedule.

