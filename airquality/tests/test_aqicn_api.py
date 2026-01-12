import pytest
import pandas as pd
import requests
from datetime import date, datetime
from airquality.util import get_pm25


class TestGetPM25:
    """Tests for the get_pm25 function with triple fallback logic."""

    def test_successful_primary_url(self, mock_requests_get):
        """Test successful PM2.5 retrieval from primary URL."""
        aqicn_url = "https://api.waqi.info/feed/@10009"
        country = "Sweden"
        city = "Stockholm"
        street = "Hornsgatan"
        day = date(2025, 1, 15)
        api_key = "test_key"

        result = get_pm25(aqicn_url, country, city, street, day, api_key)

        # Verify result is a DataFrame
        assert isinstance(result, pd.DataFrame)

        # Verify DataFrame has correct columns
        assert 'pm25' in result.columns
        assert 'country' in result.columns
        assert 'city' in result.columns
        assert 'street' in result.columns
        assert 'date' in result.columns
        assert 'url' in result.columns

        # Verify data values
        assert result['pm25'].iloc[0] == 42.5
        assert result['country'].iloc[0] == country
        assert result['city'].iloc[0] == city
        assert result['street'].iloc[0] == street
        assert result['url'].iloc[0] == aqicn_url

    def test_fallback_to_country_street(self, mock_requests_get_unknown_station):
        """Test fallback to country/street URL when primary returns 'Unknown station'."""
        aqicn_url = "https://api.waqi.info/feed/@10009"
        country = "Sweden"
        city = "Stockholm"
        street = "Hornsgatan"
        day = date(2025, 1, 15)
        api_key = "test_key"

        result = get_pm25(aqicn_url, country, city, street, day, api_key)

        # Verify we got data (after fallback)
        assert isinstance(result, pd.DataFrame)
        assert result['pm25'].iloc[0] == 42.5

        # Verify two API calls were made (primary + first fallback)
        assert mock_requests_get_unknown_station.call_count['count'] == 2

    def test_fallback_to_full_path(self, mock_requests_get_double_fallback):
        """Test fallback to country/city/street URL when both previous attempts fail."""
        aqicn_url = "https://api.waqi.info/feed/@10009"
        country = "Sweden"
        city = "Stockholm"
        street = "Hornsgatan"
        day = date(2025, 1, 15)
        api_key = "test_key"

        result = get_pm25(aqicn_url, country, city, street, day, api_key)

        # Verify we got data (after double fallback)
        assert isinstance(result, pd.DataFrame)
        assert result['pm25'].iloc[0] == 42.5

        # Verify three API calls were made (primary + 2 fallbacks)
        assert mock_requests_get_double_fallback.call_count['count'] == 3

        # Verify all three responses were "Unknown station" then success
        responses = mock_requests_get_double_fallback.responses
        assert responses[0]['data'] == 'Unknown station'
        assert responses[1]['data'] == 'Unknown station'
        assert isinstance(responses[2]['data'], dict)

    def test_missing_pm25_handling(self, mock_requests_get_no_pm25):
        """Test handling of response without pm25 data."""
        aqicn_url = "https://api.waqi.info/feed/@10009"
        country = "Sweden"
        city = "Stockholm"
        street = "Hornsgatan"
        day = date(2025, 1, 15)
        api_key = "test_key"

        result = get_pm25(aqicn_url, country, city, street, day, api_key)

        # Verify result is a DataFrame
        assert isinstance(result, pd.DataFrame)

        # Verify pm25 is None when not present in response
        assert pd.isna(result['pm25'].iloc[0])

    def test_dataframe_structure(self, mock_requests_get):
        """Test that DataFrame has correct structure and data types."""
        aqicn_url = "https://api.waqi.info/feed/@10009"
        country = "Sweden"
        city = "Stockholm"
        street = "Hornsgatan"
        day = date(2025, 1, 15)
        api_key = "test_key"

        result = get_pm25(aqicn_url, country, city, street, day, api_key)

        # Verify DataFrame shape
        assert len(result) == 1  # Should have exactly 1 row

        # Verify column count
        assert len(result.columns) == 6  # pm25, country, city, street, date, url

        # Verify data types
        assert result['pm25'].dtype == 'float32'
        assert result['country'].dtype == 'object'
        assert result['city'].dtype == 'object'
        assert result['street'].dtype == 'object'
        assert pd.api.types.is_datetime64_any_dtype(result['date'])
        assert result['url'].dtype == 'object'

    def test_date_conversion(self, mock_requests_get):
        """Test that date is correctly converted to datetime."""
        aqicn_url = "https://api.waqi.info/feed/@10009"
        country = "Sweden"
        city = "Stockholm"
        street = "Hornsgatan"
        day = date(2025, 1, 15)
        api_key = "test_key"

        result = get_pm25(aqicn_url, country, city, street, day, api_key)

        # Verify date column is datetime type
        assert pd.api.types.is_datetime64_any_dtype(result['date'])

        # Verify date value matches input
        expected_date = pd.to_datetime(day)
        assert result['date'].iloc[0] == expected_date

    def test_api_error_handling(self, monkeypatch):
        """Test that non-ok status raises RequestException."""
        def mock_get_error(url):
            mock_response = type('MockResponse', (), {})()
            mock_response.status_code = 200
            mock_response.json = lambda: {
                'status': 'error',
                'data': 'Invalid API key'
            }
            return mock_response

        monkeypatch.setattr('requests.get', mock_get_error)

        aqicn_url = "https://api.waqi.info/feed/@10009"
        country = "Sweden"
        city = "Stockholm"
        street = "Hornsgatan"
        day = date(2025, 1, 15)
        api_key = "invalid_key"

        with pytest.raises(requests.exceptions.RequestException):
            get_pm25(aqicn_url, country, city, street, day, api_key)

    def test_pm25_value_range(self, mock_requests_get):
        """Test that PM2.5 values are within realistic range."""
        aqicn_url = "https://api.waqi.info/feed/@10009"
        country = "Sweden"
        city = "Stockholm"
        street = "Hornsgatan"
        day = date(2025, 1, 15)
        api_key = "test_key"

        result = get_pm25(aqicn_url, country, city, street, day, api_key)

        # PM2.5 should be a positive value (in our mock it's 42.5)
        pm25_value = result['pm25'].iloc[0]
        assert pm25_value >= 0
        assert pm25_value < 1000  # Reasonable upper bound

    def test_url_parameter_passed(self, mock_requests_get):
        """Test that the original URL is stored in the DataFrame."""
        aqicn_url = "https://api.waqi.info/feed/@10009"
        country = "Sweden"
        city = "Stockholm"
        street = "Hornsgatan"
        day = date(2025, 1, 15)
        api_key = "test_key"

        result = get_pm25(aqicn_url, country, city, street, day, api_key)

        # Verify the URL is stored correctly
        assert result['url'].iloc[0] == aqicn_url
