import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from airquality.util import get_historical_weather, get_hourly_weather_forecast


class TestGetHistoricalWeather:
    """Tests for the get_historical_weather function."""

    def test_successful_retrieval(self, mock_openmeteo_client):
        """Test successful historical weather data retrieval."""
        city = "Stockholm"
        start_date = "2025-01-01"
        end_date = "2025-01-31"
        latitude = 59.33
        longitude = 18.07

        result = get_historical_weather(city, start_date, end_date, latitude, longitude)

        # Verify result is a DataFrame
        assert isinstance(result, pd.DataFrame)

        # Verify DataFrame is not empty
        assert len(result) > 0

    def test_dataframe_structure(self, mock_openmeteo_client):
        """Test that DataFrame has correct structure."""
        city = "Stockholm"
        start_date = "2025-01-01"
        end_date = "2025-01-31"
        latitude = 59.33
        longitude = 18.07

        result = get_historical_weather(city, start_date, end_date, latitude, longitude)

        # Verify required columns exist
        required_columns = [
            'date',
            'temperature_2m_mean',
            'precipitation_sum',
            'wind_speed_10m_max',
            'wind_direction_10m_dominant',
            'city'
        ]
        for col in required_columns:
            assert col in result.columns, f"Column '{col}' not found in DataFrame"

    def test_column_data_types(self, mock_openmeteo_client):
        """Test that columns have correct data types."""
        city = "Stockholm"
        start_date = "2025-01-01"
        end_date = "2025-01-31"
        latitude = 59.33
        longitude = 18.07

        result = get_historical_weather(city, start_date, end_date, latitude, longitude)

        # Verify data types
        assert pd.api.types.is_datetime64_any_dtype(result['date'])
        assert result['temperature_2m_mean'].dtype in [np.float32, np.float64]
        assert result['precipitation_sum'].dtype in [np.float32, np.float64]
        assert result['wind_speed_10m_max'].dtype in [np.float32, np.float64]
        assert result['wind_direction_10m_dominant'].dtype in [np.float32, np.float64]
        assert result['city'].dtype == 'object'

    def test_city_column_added(self, mock_openmeteo_client):
        """Test that city column is correctly added."""
        city = "Stockholm"
        start_date = "2025-01-01"
        end_date = "2025-01-31"
        latitude = 59.33
        longitude = 18.07

        result = get_historical_weather(city, start_date, end_date, latitude, longitude)

        # Verify city column exists and has correct value
        assert 'city' in result.columns
        assert all(result['city'] == city)

    def test_date_range(self, mock_openmeteo_client):
        """Test that date range is correctly generated."""
        city = "Stockholm"
        start_date = "2025-01-01"
        end_date = "2025-01-31"
        latitude = 59.33
        longitude = 18.07

        result = get_historical_weather(city, start_date, end_date, latitude, longitude)

        # Verify we have approximately the right number of days
        # (30 days for Jan 1-31, might be less due to dropna)
        assert len(result) > 0
        assert len(result) <= 31

    def test_no_nan_values(self, mock_openmeteo_client):
        """Test that dropna() removes rows with missing values."""
        city = "Stockholm"
        start_date = "2025-01-01"
        end_date = "2025-01-31"
        latitude = 59.33
        longitude = 18.07

        result = get_historical_weather(city, start_date, end_date, latitude, longitude)

        # Verify no NaN values exist (due to dropna)
        assert not result.isnull().any().any()

    def test_coordinate_handling(self, mock_openmeteo_client):
        """Test that different coordinates work correctly."""
        city = "London"
        start_date = "2025-01-01"
        end_date = "2025-01-10"
        latitude = 51.51
        longitude = -0.13

        result = get_historical_weather(city, start_date, end_date, latitude, longitude)

        # Verify result is valid
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0
        assert all(result['city'] == city)

    def test_weather_value_ranges(self, mock_openmeteo_client):
        """Test that weather values are within realistic ranges."""
        city = "Stockholm"
        start_date = "2025-01-01"
        end_date = "2025-01-31"
        latitude = 59.33
        longitude = 18.07

        result = get_historical_weather(city, start_date, end_date, latitude, longitude)

        # Temperature should be in reasonable range for Stockholm
        assert all(result['temperature_2m_mean'] > -50)
        assert all(result['temperature_2m_mean'] < 50)

        # Precipitation should be non-negative
        assert all(result['precipitation_sum'] >= 0)

        # Wind speed should be non-negative
        assert all(result['wind_speed_10m_max'] >= 0)

        # Wind direction should be 0-360
        assert all(result['wind_direction_10m_dominant'] >= 0)
        assert all(result['wind_direction_10m_dominant'] <= 360)


class TestGetHourlyWeatherForecast:
    """Tests for the get_hourly_weather_forecast function."""

    def test_successful_forecast_retrieval(self, mock_openmeteo_client):
        """Test successful hourly weather forecast retrieval."""
        city = "Stockholm"
        latitude = 59.33
        longitude = 18.07

        result = get_hourly_weather_forecast(city, latitude, longitude)

        # Verify result is a DataFrame
        assert isinstance(result, pd.DataFrame)

        # Verify DataFrame is not empty
        assert len(result) > 0

    def test_dataframe_structure(self, mock_openmeteo_client):
        """Test that DataFrame has correct structure."""
        city = "Stockholm"
        latitude = 59.33
        longitude = 18.07

        result = get_hourly_weather_forecast(city, latitude, longitude)

        # Verify required columns exist (note: hourly data uses daily column names)
        required_columns = [
            'date',
            'temperature_2m_mean',  # Renamed from temperature_2m
            'precipitation_sum',    # Renamed from precipitation
            'wind_speed_10m_max',   # Renamed from wind_speed_10m
            'wind_direction_10m_dominant'  # Renamed from wind_direction_10m
        ]
        for col in required_columns:
            assert col in result.columns, f"Column '{col}' not found in DataFrame"

    def test_column_naming_convention(self, mock_openmeteo_client):
        """Test that hourly data uses daily naming conventions."""
        city = "Stockholm"
        latitude = 59.33
        longitude = 18.07

        result = get_hourly_weather_forecast(city, latitude, longitude)

        # These are the renamed columns (hourly → daily convention)
        assert 'temperature_2m_mean' in result.columns
        assert 'precipitation_sum' in result.columns
        assert 'wind_speed_10m_max' in result.columns
        assert 'wind_direction_10m_dominant' in result.columns

    def test_hourly_data_length(self, mock_openmeteo_client):
        """Test that hourly forecast returns multiple hours of data."""
        city = "Stockholm"
        latitude = 59.33
        longitude = 18.07

        result = get_hourly_weather_forecast(city, latitude, longitude)

        # Should have many hours of forecast data (typically 240 hours = 10 days)
        # Might be less due to dropna()
        assert len(result) > 0

    def test_no_nan_values(self, mock_openmeteo_client):
        """Test that dropna() removes rows with missing values."""
        city = "Stockholm"
        latitude = 59.33
        longitude = 18.07

        result = get_hourly_weather_forecast(city, latitude, longitude)

        # Verify no NaN values exist (due to dropna)
        assert not result.isnull().any().any()

    def test_coordinate_handling(self, mock_openmeteo_client):
        """Test that different coordinates work correctly."""
        city = "New York"
        latitude = 40.71
        longitude = -74.01

        result = get_hourly_weather_forecast(city, latitude, longitude)

        # Verify result is valid
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

    def test_weather_value_ranges(self, mock_openmeteo_client):
        """Test that forecast values are within realistic ranges."""
        city = "Stockholm"
        latitude = 59.33
        longitude = 18.07

        result = get_hourly_weather_forecast(city, latitude, longitude)

        # Temperature should be in reasonable range
        assert all(result['temperature_2m_mean'] > -50)
        assert all(result['temperature_2m_mean'] < 50)

        # Precipitation should be non-negative
        assert all(result['precipitation_sum'] >= 0)

        # Wind speed should be non-negative
        assert all(result['wind_speed_10m_max'] >= 0)

        # Wind direction should be 0-360
        assert all(result['wind_direction_10m_dominant'] >= 0)
        assert all(result['wind_direction_10m_dominant'] <= 360)

    def test_date_column_type(self, mock_openmeteo_client):
        """Test that date column is datetime type."""
        city = "Stockholm"
        latitude = 59.33
        longitude = 18.07

        result = get_hourly_weather_forecast(city, latitude, longitude)

        # Verify date column is datetime type
        assert pd.api.types.is_datetime64_any_dtype(result['date'])
