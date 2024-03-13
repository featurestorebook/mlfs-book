import pandas as pd
from typing import Any, Dict, List
import datetime
import pandas as pd


def get_data_for_date(date: str, feature_view, model) -> pd.DataFrame:
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
    
    # Retrieve batch data for the specified date range
    batch_data = feature_view.get_batch_data(
        start_time=date_datetime - datetime.timedelta(days=1),
        end_time=date_datetime,
    )
    
    batch_data['date'] = batch_data['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
    
    return batch_data[['date', 'pm25']].sort_values('date').reset_index(drop=True)


def get_data_in_date_range(date_start: str, date_end: str, feature_view, model) -> pd.DataFrame:
    """
    Retrieve data for a specific date range from a feature view.

    Args:
        date_start (str): The start date in the format "%Y-%m-%d".
        date_end (str): The end date in the format "%Y-%m-%d".
        feature_view: The feature view object.
        model: The machine learning model used for prediction.

    Returns:
        pd.DataFrame: A DataFrame containing data for the specified date range.
    """
    # Convert date strings to datetime objects
    date_start_dt = datetime.datetime.strptime(date_start, "%Y-%m-%d").date()
    date_end_dt = datetime.datetime.strptime(date_end, "%Y-%m-%d").date()
    
    # Retrieve batch data for the specified date range
    batch_data = feature_view.get_batch_data(
        start_time=date_start_dt - datetime.timedelta(days=1),
        end_time=date_end_dt,
    )
    
    batch_data['date'] = batch_data['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        
    return batch_data[['date', 'pm25']].sort_values('date').reset_index(drop=True)


def get_future_data(date: str, feature_view, model) -> pd.DataFrame:
    """
    Predicts future PM2.5 data for a specified date using a given feature view and model.

    Args:
        date (str): The target future date in the format 'YYYY-MM-DD'.
        feature_view: The feature view used to retrieve batch data.
        model: The machine learning model used for prediction.

    Returns:
        pd.DataFrame: A DataFrame containing predicted PM2.5 values for each day starting from the target date.

    """
    # Get today's date
    today = datetime.date.today()

    # Convert the target date string to a datetime object
    date_in_future = datetime.datetime.strptime(date, "%Y-%m-%d").date()

    # Calculate the difference in days between today and the target date
    difference_in_days = (date_in_future - today).days
    
    # Retrieve batch data for the specified date range
    batch_data = feature_view.get_batch_data(
        start_time=today - datetime.timedelta(days=1),
        end_time=today,
    )
                
    # Initialize a DataFrame to store predicted PM2.5 values
    try:
        pm25_value = batch_data['pm25'].values[0]
    except (IndexError, TypeError):
        # If accessing pm25 values fails, return a message indicating the feature pipeline needs updating
        return "Data is not available. Ask user to run the feature pipeline to update data."
    
    # Initialize a DataFrame to store predicted PM2.5 values
    predicted_pm25_df = pd.DataFrame({
        'date': [today.strftime("%Y-%m-%d")],
        'pm25': pm25_value,
    })

    batch_data.drop(['date', 'pm25'] , axis=1, inplace=True)
        
    # Iterate through each day starting from tomorrow up to the target date
    for day_number in range(1, difference_in_days + 1):

        # Calculate the date for the current future day
        date_future_day = (today + datetime.timedelta(days=day_number)).strftime("%Y-%m-%d")
        
        # Predict PM2.5 for the current day
        predicted_pm2_5 = model.predict(batch_data)
        
        # Append the predicted PM2.5 value for the current day to the DataFrame
        predicted_pm25_df = predicted_pm25_df._append(
            {'date': date_future_day, 'pm25': predicted_pm2_5[0],}, 
            ignore_index=True,
        )
        
    return predicted_pm25_df
