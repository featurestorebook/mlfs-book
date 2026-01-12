import sys
from unittest.mock import Mock, MagicMock
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta

# ====================================================================
# Module-level mocks (must be set before importing airquality modules)
# ====================================================================

# Mock hsfs module and submodules
hsfs_mock = MagicMock()
hsfs_client_mock = MagicMock()
hsfs_client_exceptions_mock = MagicMock()
hsfs_client_exceptions_mock.RestAPIError = Exception
hsfs_client_mock.exceptions = hsfs_client_exceptions_mock
hsfs_mock.client = hsfs_client_mock

# Mock hopsworks module
hopsworks_mock = MagicMock()
hopsworks_core_mock = MagicMock()
hopsworks_client_exceptions_mock = MagicMock()
hopsworks_client_exceptions_mock.RestAPIError = Exception
hopsworks_client_mock = MagicMock()
hopsworks_client_mock.exceptions = hopsworks_client_exceptions_mock
hopsworks_mock.client = hopsworks_client_mock
hopsworks_mock.core = hopsworks_core_mock

# Install the mocks
sys.modules['hsfs'] = hsfs_mock
sys.modules['hsfs.client'] = hsfs_client_mock
sys.modules['hsfs.client.exceptions'] = hsfs_client_exceptions_mock
sys.modules['hopsworks'] = hopsworks_mock
sys.modules['hopsworks.core'] = hopsworks_core_mock
sys.modules['hopsworks.client'] = hopsworks_client_mock
sys.modules['hopsworks.client.exceptions'] = hopsworks_client_exceptions_mock

# ====================================================================
# Mock Classes for API Responses
# ====================================================================

class MockOpenMeteoVariable:
    """Mock for Open-Meteo Variable object."""
    def __init__(self, index, data_length=30):
        self.index = index
        self.data_length = data_length

    def ValuesAsNumpy(self):
        """Return realistic weather data based on variable index."""
        if self.index == 0:  # temperature_2m_mean
            return np.random.uniform(10, 25, self.data_length).astype(np.float32)
        elif self.index == 1:  # precipitation_sum
            return np.random.uniform(0, 10, self.data_length).astype(np.float32)
        elif self.index == 2:  # wind_speed_10m_max
            return np.random.uniform(5, 20, self.data_length).astype(np.float32)
        elif self.index == 3:  # wind_direction_10m_dominant
            return np.random.uniform(0, 360, self.data_length).astype(np.float32)
        else:
            return np.random.rand(self.data_length).astype(np.float32)


class MockDailyData:
    """Mock for Open-Meteo Daily data object."""
    def __init__(self, start_date=None, days=30):
        if start_date is None:
            start_date = datetime(2025, 1, 1)
        self.start_date = start_date
        self.days = days
        self.end_date = start_date + timedelta(days=days)

    def Time(self):
        return int(self.start_date.timestamp())

    def TimeEnd(self):
        return int(self.end_date.timestamp())

    def Interval(self):
        return 86400  # 1 day in seconds

    def Variables(self, index):
        return MockOpenMeteoVariable(index, self.days)


class MockHourlyData:
    """Mock for Open-Meteo Hourly data object."""
    def __init__(self, start_date=None, hours=240):
        if start_date is None:
            start_date = datetime(2025, 1, 1)
        self.start_date = start_date
        self.hours = hours
        self.end_date = start_date + timedelta(hours=hours)

    def Time(self):
        return int(self.start_date.timestamp())

    def TimeEnd(self):
        return int(self.end_date.timestamp())

    def Interval(self):
        return 3600  # 1 hour in seconds

    def Variables(self, index):
        return MockOpenMeteoVariable(index, self.hours)


class MockOpenMeteoResponse:
    """Mock for Open-Meteo API response object."""
    def __init__(self, latitude=59.33, longitude=18.07, mode='daily', days=30, hours=240):
        self._latitude = latitude
        self._longitude = longitude
        self._mode = mode
        self._days = days
        self._hours = hours

    def Latitude(self):
        return self._latitude

    def Longitude(self):
        return self._longitude

    def Elevation(self):
        return 10.0

    def Timezone(self):
        return "GMT"

    def TimezoneAbbreviation(self):
        return "GMT"

    def UtcOffsetSeconds(self):
        return 0

    def Daily(self):
        return MockDailyData(days=self._days)

    def Hourly(self):
        return MockHourlyData(hours=self._hours)


class MockLocation:
    """Mock for geopy Location object."""
    def __init__(self, city_name):
        self.city_name = city_name
        # Predefined coordinates for test cities
        coords = {
            'Stockholm': (59.3293, 18.0686),
            'London': (51.5074, -0.1278),
            'New York': (40.7128, -74.0060),
            'Paris': (48.8566, 2.3522),
        }
        if city_name in coords:
            self.latitude, self.longitude = coords[city_name]
        else:
            self.latitude, self.longitude = (0.0, 0.0)


# ====================================================================
# pytest Fixtures
# ====================================================================

@pytest.fixture
def mock_openmeteo_client(monkeypatch):
    """Mock the openmeteo_requests.Client class."""
    mock_client = MagicMock()

    def mock_weather_api(url, params=None):
        # Return a mock response based on the URL
        if 'archive-api.open-meteo.com' in url:
            return [MockOpenMeteoResponse(mode='daily')]
        elif 'api.open-meteo.com' in url:
            return [MockOpenMeteoResponse(mode='hourly', hours=240)]
        else:
            return [MockOpenMeteoResponse()]

    mock_client.weather_api = mock_weather_api

    # Mock the Client class constructor
    def mock_client_constructor(session=None):
        return mock_client

    monkeypatch.setattr('openmeteo_requests.Client', mock_client_constructor)
    return mock_client


@pytest.fixture
def mock_requests_get(monkeypatch):
    """Mock requests.get for AQICN API calls."""
    call_count = {'count': 0}

    def mock_get(url):
        call_count['count'] += 1
        mock_response = MagicMock()
        mock_response.status_code = 200

        # Default successful response
        mock_response.json.return_value = {
            'status': 'ok',
            'data': {
                'iaqi': {
                    'pm25': {'v': 42.5}
                }
            }
        }

        return mock_response

    mock_get.call_count = call_count
    monkeypatch.setattr('requests.get', mock_get)
    return mock_get


@pytest.fixture
def mock_requests_get_unknown_station(monkeypatch):
    """Mock requests.get that returns 'Unknown station' for fallback testing."""
    call_count = {'count': 0}
    responses = []

    def mock_get(url):
        call_count['count'] += 1
        mock_response = MagicMock()
        mock_response.status_code = 200

        # First call returns "Unknown station", second call succeeds
        if call_count['count'] == 1:
            response_data = {
                'status': 'ok',
                'data': 'Unknown station'
            }
        else:
            response_data = {
                'status': 'ok',
                'data': {
                    'iaqi': {
                        'pm25': {'v': 42.5}
                    }
                }
            }

        responses.append(response_data)
        mock_response.json.return_value = response_data
        return mock_response

    mock_get.call_count = call_count
    mock_get.responses = responses
    monkeypatch.setattr('requests.get', mock_get)
    return mock_get


@pytest.fixture
def mock_requests_get_double_fallback(monkeypatch):
    """Mock requests.get that requires two fallbacks."""
    call_count = {'count': 0}
    responses = []

    def mock_get(url):
        call_count['count'] += 1
        mock_response = MagicMock()
        mock_response.status_code = 200

        # First two calls return "Unknown station", third succeeds
        if call_count['count'] <= 2:
            response_data = {
                'status': 'ok',
                'data': 'Unknown station'
            }
        else:
            response_data = {
                'status': 'ok',
                'data': {
                    'iaqi': {
                        'pm25': {'v': 42.5}
                    }
                }
            }

        responses.append(response_data)
        mock_response.json.return_value = response_data
        return mock_response

    mock_get.call_count = call_count
    mock_get.responses = responses
    monkeypatch.setattr('requests.get', mock_get)
    return mock_get


@pytest.fixture
def mock_requests_get_no_pm25(monkeypatch):
    """Mock requests.get that returns response without pm25 data."""
    def mock_get(url):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'status': 'ok',
            'data': {
                'iaqi': {}  # No pm25 key
            }
        }
        return mock_response

    monkeypatch.setattr('requests.get', mock_get)
    return mock_get


@pytest.fixture
def mock_requests_get_error(monkeypatch):
    """Mock requests.get that returns an error status."""
    def mock_get(url):
        mock_response = MagicMock()
        mock_response.status_code = 404
        return mock_response

    monkeypatch.setattr('requests.get', mock_get)
    return mock_get


@pytest.fixture
def mock_geocoder(monkeypatch):
    """Mock geopy Nominatim geocoder."""
    class MockNominatim:
        def __init__(self, user_agent):
            self.user_agent = user_agent

        def geocode(self, city_name):
            return MockLocation(city_name)

    # Patch at the import location used by airquality.util
    monkeypatch.setattr('airquality.util.Nominatim', MockNominatim)
    return MockNominatim


@pytest.fixture
def sample_weather_df():
    """Pre-built weather DataFrame for testing."""
    dates = pd.date_range('2025-01-01', periods=30, freq='D')
    return pd.DataFrame({
        'date': dates,
        'temperature_2m_mean': np.random.uniform(10, 25, 30).astype(np.float32),
        'precipitation_sum': np.random.uniform(0, 10, 30).astype(np.float32),
        'wind_speed_10m_max': np.random.uniform(5, 20, 30).astype(np.float32),
        'wind_direction_10m_dominant': np.random.uniform(0, 360, 30).astype(np.float32),
        'city': ['Stockholm'] * 30
    })


@pytest.fixture
def sample_aq_df():
    """Pre-built air quality DataFrame for testing."""
    dates = pd.date_range('2025-01-01', periods=10, freq='D')
    return pd.DataFrame({
        'date': dates,
        'pm25': np.random.uniform(10, 100, 10).astype(np.float32),
        'country': ['Sweden'] * 10,
        'city': ['Stockholm'] * 10,
        'street': ['Hornsgatan'] * 10,
    })


@pytest.fixture
def sample_forecast_df():
    """Pre-built forecast DataFrame for plotting tests."""
    dates = pd.date_range('2025-01-01', periods=10, freq='D')
    return pd.DataFrame({
        'date': dates,
        'predicted_pm25': np.random.uniform(20, 80, 10).astype(np.float32),
    })


@pytest.fixture
def sample_hindcast_df():
    """Pre-built hindcast DataFrame with actual pm25 for plotting tests."""
    dates = pd.date_range('2025-01-01', periods=10, freq='D')
    return pd.DataFrame({
        'date': dates,
        'predicted_pm25': np.random.uniform(20, 80, 10).astype(np.float32),
        'pm25': np.random.uniform(15, 85, 10).astype(np.float32),
    })


@pytest.fixture
def mock_feature_group():
    """Mock Hopsworks feature group."""
    mock_fg = MagicMock()
    mock_fg.read.return_value = pd.DataFrame({
        'date': pd.date_range('2025-01-01', periods=30, freq='D'),
        'temperature_2m_mean': np.random.uniform(10, 25, 30).astype(np.float32),
        'precipitation_sum': np.random.uniform(0, 10, 30).astype(np.float32),
        'wind_speed_10m_max': np.random.uniform(5, 20, 30).astype(np.float32),
        'wind_direction_10m_dominant': np.random.uniform(0, 360, 30).astype(np.float32),
        'city': ['Stockholm'] * 30
    })
    mock_fg.insert.return_value = None
    return mock_fg


@pytest.fixture
def mock_model():
    """Mock ML model for predictions."""
    mock_mdl = MagicMock()

    def predict(X):
        # Return realistic PM2.5 predictions based on input size
        return np.random.uniform(20, 80, len(X)).astype(np.float32)

    mock_mdl.predict = predict
    return mock_mdl
