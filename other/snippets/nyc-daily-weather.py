import pandas as pd
import requests
from datetime import datetime, timedelta
import hsfs
from hopsworks import login
import modal

# Define the Modal stub and image
app = modal.App("nyc-weather-pipeline")

# Create image with required dependencies
image = modal.Image.debian_slim().pip_install(
    "requests",
    "hopsworks"
)

# Define secrets

def fetch_yesterday_weather() -> pd.DataFrame:
    """
    Fetch weather data for yesterday only.
    
    Returns:
        pd.DataFrame: Weather data for yesterday
    """
    # Calculate yesterday's date
    yesterday = (datetime.now() - timedelta(days=1)).date()
    
    print(f"Fetching weather data for {yesterday}")
    
    # Open-Meteo API endpoint for NYC (latitude: 40.7128, longitude: -74.0060)
    url = (
        "https://archive-api.open-meteo.com/v1/archive"
        "?latitude=40.7128"
        "&longitude=-74.0060"
        f"&start_date={yesterday}"
        f"&end_date={yesterday}"
        "&daily=temperature_2m_max"
        "&daily=temperature_2m_min"
        "&daily=precipitation_sum"
        "&daily=windspeed_10m_max"
        "&daily=winddirection_10m_dominant"
        "&timezone=America%2FNew_York"
    )
    
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"API request failed with status code: {response.status_code}")
    
    data = response.json()
    
    # Validate that we got exactly one day of data
    if len(data['daily']['time']) != 1:
        raise ValueError(f"Expected 1 day of data, got {len(data['daily']['time'])} days")
    
    # Create DataFrame
    df = pd.DataFrame({
        'date': pd.to_datetime(data['daily']['time']),
        'temp_max': data['daily']['temperature_2m_max'],
        'temp_min': data['daily']['temperature_2m_min'],
        'precipitation': data['daily']['precipitation_sum'],
        'wind_speed_max': data['daily']['windspeed_10m_max'],
        'wind_direction': data['daily']['winddirection_10m_dominant']
    })
    
    return df

def save_to_hopsworks(df: pd.DataFrame) -> None:
    """
    Save weather data to Hopsworks feature store.
    
    Args:
        df (pd.DataFrame): Weather data to save
    """
    # Connect to Hopsworks using API key from secrets
    project = login()
    fs = project.get_feature_store()
    
    # Create feature group
    weather_fg = fs.get_or_create_feature_group(
        name='nyc_weather_daily',
        version=1,
        primary_key=['date'],
        description='Daily weather data for NYC from Open-Meteo'
    )
    
    # Upload the data
    weather_fg.insert(df, write_options={"start_offline_backfill": True})

@app.function(
    image=image,
    schedule=modal.Period(days=1),
    secret=[modal.Secret.from_name("HOPSWORKS_API_KEY")],
    cpu=1,
    memory=2048
)
def run_pipeline() -> bool:
    """
    Run the weather data pipeline to fetch and store yesterday's weather data.
            
    Returns:
        bool: True if pipeline runs successfully
    """
    try:
        print(f"Starting weather data pipeline at {datetime.now()}")
        
        # Fetch yesterday's weather data
        weather_df = fetch_yesterday_weather()
        
        # Basic data validation
        if weather_df.empty:
            raise Exception("No data received from API")
        
        # Validate weather values
        if not (weather_df['temp_max'] >= weather_df['temp_min']).all():
            raise ValueError("Invalid temperature data: max temperature is less than min temperature")
        
        if (weather_df['precipitation'] < 0).any():
            raise ValueError("Invalid precipitation data: negative values found")
        
        if not (0 <= weather_df['wind_direction']).all() <= 360:
            raise ValueError("Invalid wind direction: values must be between 0 and 360 degrees")
            
        print("Data validation passed successfully")
        
        # Save to Hopsworks
        print("Saving data to Hopsworks...")
        save_to_hopsworks(weather_df)
        
        print(f"Successfully saved weather data for {weather_df['date'].iloc[0].date()}")
        
        return True
        
    except Exception as e:
        print(f"Error in data pipeline: {str(e)}")
        raise

# For local development and testing
if __name__ == "__main__":
    with app.run():
        run_pipeline.remote()
