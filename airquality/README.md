## Air Quality Predcition Service

This project builds an air quality prediction service using PM 2.5 measurements taken from public air quality sensor(s) as well as weather data for the sensor location(s). The trained model (XGBoost) produces a 7-day air quality forecast using weather forecast data as input features. The output is a dashboard showing air quality forecast(s) for the next 7 days.


## How to run Instructions

    source set-env.sh
    inv backfill
    inv features
    inv train
    inv inference 
