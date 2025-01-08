import pandas as pd
import requests
from datetime import datetime, timedelta
import hsfs
from hopsworks import login

def fetch_weather_data():
    # Calculate date range (3 years from today)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=3*365)
    
    # Open-Meteo API endpoint for NYC (latitude: 40.7128, longitude: -74.0060)
    url = (
        "https://archive-api.open-meteo.com/v1/archive"
        "?latitude=40.7128"
        "&longitude=-74.0060"
        f"&start_date={start_date.strftime('%Y-%m-%d')}"
        f"&end_date={end_date.strftime('%Y-%m-%d')}"
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

def save_to_hopsworks(df):
    # Connect to Hopsworks
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
    weather_fg.insert(df)
    
def main():
    try:
        # Fetch weather data
        print("Fetching weather data from Open-Meteo...")
        weather_df = fetch_weather_data()
        
        # Basic data validation
        if weather_df.empty:
            raise Exception("No data received from API")
        
        print(f"Successfully fetched {len(weather_df)} days of weather data")
        
        # Save to Hopsworks
        print("Saving data to Hopsworks...")
        save_to_hopsworks(weather_df)
        
        print("Data pipeline completed successfully!")
        
        return weather_df
        
    except Exception as e:
        print(f"Error in data pipeline: {str(e)}")
        raise

if __name__ == "__main__":
    main()
