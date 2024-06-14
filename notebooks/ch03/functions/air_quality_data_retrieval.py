import pandas as pd
from typing import Any, Dict, List
import datetime
import pandas as pd
import hopsworks
from hsfs.feature import Feature

def get_historical_data_for_date(date: str, feature_view, weather_fg, model) -> pd.DataFrame:
    """
    Retrieve data for a specific date from a feature view.

    Args:
        date (str): The date in the format "%Y-%m-%d".
        feature_view: The feature view object.
        model: The machine learning model used for prediction.

    Returns:
        pd.DataFrame: A DataFrame containing data for the specified date.
    """
    # Convert date string to datetime object
    date_datetime = datetime.datetime.strptime(date, "%Y-%m-%d").date()
    
    features_df, labels_df = feature_view.training_data(
        start_time=date_datetime,
        end_time=date_datetime + datetime.timedelta(days=1), 
        # event_time=True,
        statistics_config=False
    )
    # bugfix line, shouldn't need to cast to datetime
    features_df['date'] = pd.to_datetime(features_df['date'])    
    batch_data = features_df
    batch_data['pm25'] = labels_df['pm25']    
    batch_data['date'] = batch_data['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
    
    return batch_data[['date', 'pm25']].sort_values('date').reset_index(drop=True)


def get_historical_data_in_date_range(date_start: str, date_end: str, feature_view,  weather_fg, model) -> pd.DataFrame:
    """
    Retrieve data for a specific date range from a time in the past from a feature view.

    Args:
        date_start (str): The start date in the format "%Y-%m-%d".
        date_end (str): The end date in the format "%Y-%m-%d".
        feature_view: The feature view object.
        model: The machine learning model used for prediction.

    Returns:
        pd.DataFrame: A DataFrame containing data for the specified date range.
    """
    # Convert date strings to datetime objects
#     date_start_dt = datetime.datetime.strptime(date_start, "%Y-%m-%d").date()
#     date_end_dt = datetime.datetime.strptime(date_end, "%Y-%m-%d").date()
  
    batch_data = feature_view.query.read()
    batch_data = batch_data[(batch_data['date'] >= date_start) & (batch_data['date'] <= date_end)]
    
    batch_data['date'] = batch_data['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        
    return batch_data[['date', 'pm25']].sort_values('date').reset_index(drop=True)

def get_future_data_for_date(date: str, feature_view,  weather_fg, model) -> pd.DataFrame:
    """
    Predicts future PM2.5 data for a specified date using a given feature view and model.
    
    Args:
        date (str): The date in the format "%Y-%m-%d".
        feature_view: The feature view object.
        model: The machine learning model used for prediction.

    Returns:
        pd.DataFrame: A DataFrame containing data for the specified date.
    """
    date_start_dt = datetime.datetime.strptime(date, "%Y-%m-%d") #.date()
    fg_data = weather_fg.read()

    # Couldn't get our filters to work, so filter in memory
    df = fg_data[fg_data.date == date_start_dt]
    batch_data = df.drop(['date', 'city'], axis=1)

    df['pm25'] = model.predict(batch_data)

    return df[['date', 'pm25']].sort_values('date').reset_index(drop=True)



def get_future_data_in_date_range(date_start: str, date_end: str, feature_view,  weather_fg, model) -> pd.DataFrame:
    """
    Predicts future PM2.5 data for a specified start and end date range using a given feature view and model.
    
    Args:
        date_start (str): The start date in the format "%Y-%m-%d".
        date_end (str): The end date in the format "%Y-%m-%d".
        feature_view: The feature view object.
        model: The machine learning model used for prediction.

    Returns:
        pd.DataFrame: A DataFrame containing data for the specified date range.
    """
    date_start_dt = datetime.datetime.strptime(date_start, "%Y-%m-%d") #.date()
    if date_end == None:
        date_end = date_start
    date_end_dt = datetime.datetime.strptime(date_end, "%Y-%m-%d") #.date()
    
    fg_data = weather_fg.read()
    # Fix bug: Cannot compare tz-naive and tz-aware datetime-like objects
    fg_data['date'] = pd.to_datetime(fg_data['date']).dt.tz_localize(None)

    # Couldn't get our filters to work, so filter in memory
    df = fg_data[(fg_data['date'] >= date_start_dt) & (fg_data['date'] <= date_end_dt)]
    batch_data = df.drop(['date', 'city'], axis=1)

    df['pm25'] = model.predict(batch_data)

    return df[['date', 'pm25']].sort_values('date').reset_index(drop=True)

