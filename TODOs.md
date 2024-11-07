# TODOs

## Set Up GitHub Actions

Notebook `2_air_quality_feature_pipeline.ipynb` should be scheduled to run daily.
We need to set up GitHub Actions stored [here](./.github/workflows/air-quality-daily.yml).

## Building the Dashboard as a GitHub Page

Build and publish a GitHub Page containing the Air Quality Forecasting Dashboard (with our PNG charts). The GitHub action YAML file contains a step called `git-auto-commit-action` that pushes the new PNG files to our GitHub repository and rebuilds the GitHub Pages.

## Parameterize the notebook 

Parameterize the notebook `1_air_quality_feature_backfill.ipynb` so that you can provide the `country`/`street`/`city`/`url`/`csv_file` as parameters. 
      * Hint: this will also require making the secret name (`SENSOR_LOCATION_JSON`), e.g., add the street name as part of the secret name. Then you have to pass that secret name as a parameter when running the operational feature pipeline and batch inference pipelines.
      * After you have done this, collect the street/city/url/csv files for all the sensors in your city or region and you make dashboards for all of the air quality sensors in your city/region. You could even then add a dashboard for your city/region, as done [here for Poland](https://github.com/erno98/ID2223).

## Implement Hyperparameter Tuning

XGBoost works well out of the box, and we won’t do any hyperparameter tuning here - we will leave it as an exercise for you to squeeze more performance out of the model.

## Enhance Training Features 

A useful exercise would be to improve this air quality model by adding features related to air quality (historical air quality, seasonality factors, and so on) and change to a time-series split.
Try adding a new feature based on a rolling window of 3 days for 'pm25'
      * This is not easy, as forecasting more than 1 day in the future, you won't have the previous 3 days of pm25 measurements.
      * df.set_index("date").rolling(3).mean() is only the start....