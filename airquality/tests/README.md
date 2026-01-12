# Air Quality Tests

This directory contains comprehensive unit tests for the air quality project's feature backfill and feature pipeline functionality.

## Running Tests

### Via Invoke Task (Recommended)
```bash
# Make sure virtual environment is activated
source .venv/bin/activate

# Run all tests
inv test
```

### Direct pytest
```bash
# Run all tests with verbose output
pytest tests/ -v

# Run specific test file
pytest tests/test_weather_api.py -v

# Run specific test class
pytest tests/test_aqicn_api.py::TestGetPM25 -v

# Run specific test
pytest tests/test_geocoding.py::TestGetCityCoordinates::test_successful_geocoding_stockholm -v

# Run with very verbose output (shows test docstrings)
pytest tests/ -vv

# Run with output capture disabled (see print statements)
pytest tests/ -v -s
```

### Coverage Reporting
```bash
# Run tests with coverage
pytest tests/ --cov=airquality --cov-report=term-missing

# Generate HTML coverage report
pytest tests/ --cov=airquality --cov-report=html

# Open coverage report in browser
open htmlcov/index.html  # macOS
xdg-open htmlcov/index.html  # Linux
```

## Test Organization

### Test Files

- **`test_geocoding.py`** - Tests for geocoding functions
  - `get_city_coordinates()` - City name to lat/lon conversion using Nominatim

- **`test_utilities.py`** - Tests for utility functions
  - `check_file_path()` - File existence validation
  - `trigger_request()` - HTTP GET requests with error handling
  - `backfill_predictions_for_monitoring()` - Prediction backfilling for monitoring

- **`test_aqicn_api.py`** - Tests for AQICN air quality API functions
  - `get_pm25()` - PM2.5 retrieval with triple fallback logic:
    1. Primary URL: `aqicn_url/?token=KEY`
    2. First fallback: `api.waqi.info/feed/{country}/{street}/?token=KEY`
    3. Second fallback: `api.waqi.info/feed/{country}/{city}/{street}/?token=KEY`

- **`test_weather_api.py`** - Tests for Open-Meteo weather API functions
  - `get_historical_weather()` - Historical weather data retrieval
  - `get_hourly_weather_forecast()` - Hourly weather forecast retrieval

- **`test_visualization.py`** - Tests for matplotlib plotting functions
  - `plot_air_quality_forecast()` - PM2.5 forecast visualization with air quality bands

### Configuration Files

- **`conftest.py`** - pytest configuration and fixtures
  - Mock infrastructure for hopsworks, hsfs, openmeteo, requests, geopy
  - Reusable test data fixtures
  - Mock API response generators

- **`__init__.py`** - Package marker for tests directory

## Mocking Strategy

All external dependencies are mocked to ensure tests:
- Run without actual API calls
- Don't require API keys or credentials
- Don't need network connectivity
- Execute quickly and determinately
- Are suitable for CI/CD environments

### Mocked Dependencies

1. **Hopsworks & HSFS** - Feature store client and SDK
   - Mocked at module level in `conftest.py`
   - Prevents requiring Hopsworks connection

2. **Open-Meteo API** - Weather data provider
   - Mock client returns realistic weather data
   - Mock response objects mimic SDK structure
   - Supports both daily and hourly data modes

3. **AQICN API** - Air quality data provider
   - Mock HTTP responses for various scenarios
   - Simulates fallback URL logic
   - Handles "Unknown station" responses

4. **Nominatim Geocoder** - City geocoding service
   - Predefined coordinates for test cities
   - No actual geocoding API calls

## Key Test Fixtures

Defined in `conftest.py`:

### API Mocks
- `mock_openmeteo_client` - Mock Open-Meteo API client
- `mock_requests_get` - Mock successful AQICN API response
- `mock_requests_get_unknown_station` - Mock response requiring first fallback
- `mock_requests_get_double_fallback` - Mock response requiring both fallbacks
- `mock_requests_get_no_pm25` - Mock response without PM2.5 data
- `mock_requests_get_error` - Mock HTTP error response
- `mock_geocoder` - Mock Nominatim geocoding service

### Test Data
- `sample_weather_df` - Pre-built weather DataFrame
- `sample_aq_df` - Pre-built air quality DataFrame
- `sample_forecast_df` - Forecast data for visualization tests
- `sample_hindcast_df` - Hindcast data with actual PM2.5 values

### Hopsworks Mocks
- `mock_feature_group` - Mock feature group with read/insert methods
- `mock_model` - Mock ML model with predict method

## Test Coverage

The tests cover all critical functions in `airquality/util.py`:

### Weather Functions
- ✅ Historical weather retrieval from Open-Meteo archive API
- ✅ Hourly weather forecast from Open-Meteo forecast API
- ✅ DataFrame structure and data type validation
- ✅ Date range handling and dropna() behavior
- ✅ City column addition

### Air Quality Functions
- ✅ PM2.5 retrieval from AQICN API
- ✅ Triple fallback URL logic for failed requests
- ✅ "Unknown station" handling
- ✅ Missing PM2.5 data handling
- ✅ DataFrame structure and data types

### Geocoding Functions
- ✅ City name to coordinates conversion
- ✅ Coordinate rounding to 2 decimal places
- ✅ Multiple city formats and names

### Utility Functions
- ✅ File path validation
- ✅ HTTP request error handling
- ✅ Prediction backfilling for monitoring
- ✅ DataFrame merge operations

### Visualization Functions
- ✅ Plot generation with/without hindcast
- ✅ File creation and PNG format
- ✅ Logarithmic y-axis scaling
- ✅ Various DataFrame sizes
- ✅ Extreme PM2.5 values

## Edge Cases Covered

- Empty or missing data in API responses
- Network errors and timeouts (simulated)
- Invalid coordinates or city names
- Malformed API responses
- Very small or very large datasets
- Extreme PM2.5 values (1 to 500)
- Missing pm25 keys in JSON responses
- Date conversion and timezone handling

## Adding New Tests

### 1. Create Test File
```python
# tests/test_new_feature.py
import pytest
from airquality.util import new_function


class TestNewFunction:
    """Tests for the new_function."""

    def test_basic_functionality(self):
        """Test basic functionality."""
        result = new_function(input_data)
        assert result == expected_output

    def test_edge_case(self):
        """Test edge case handling."""
        with pytest.raises(ValueError):
            new_function(invalid_input)
```

### 2. Add Fixtures if Needed
```python
# tests/conftest.py
@pytest.fixture
def new_mock_data():
    """Mock data for new feature."""
    return {...}
```

### 3. Run New Tests
```bash
pytest tests/test_new_feature.py -v
```

## Test Patterns

### Class-Based Organization
```python
class TestFunctionName:
    """Group related tests together."""

    def test_success_case(self):
        """Test successful execution."""
        pass

    def test_error_case(self):
        """Test error handling."""
        pass
```

### Parametrized Tests
```python
@pytest.mark.parametrize("input,expected", [
    ("Stockholm", (59.33, 18.07)),
    ("London", (51.51, -0.13)),
])
def test_multiple_inputs(input, expected):
    result = function(input)
    assert result == expected
```

### Exception Testing
```python
def test_raises_exception(self):
    with pytest.raises(ValueError):
        function_that_raises(bad_input)
```

### Fixture Usage
```python
def test_with_fixture(self, mock_api, sample_data):
    result = function(sample_data)
    assert result is not None
```

## Debugging Tests

### View Print Statements
```bash
pytest tests/test_file.py -v -s
```

### Stop on First Failure
```bash
pytest tests/ -x
```

### Run Last Failed Tests
```bash
pytest tests/ --lf
```

### Verbose Traceback
```bash
pytest tests/ -vv --tb=long
```

### PDB Debugger on Failure
```bash
pytest tests/ --pdb
```

## CI/CD Integration

These tests are designed to run in CI/CD environments:

### Requirements
- No API keys needed
- No network access required
- No external service dependencies
- Fast execution (< 10 seconds)

### GitHub Actions Example
```yaml
- name: Run tests
  run: |
    source .venv/bin/activate
    inv test
```

### Expected Behavior
- All tests should pass without environment variables
- No real API calls are made
- Tests run in isolated environment

## Troubleshooting

### Import Errors
```bash
# Make sure you're in the right directory
cd /home/jdowling/Projects/mlfs-book/airquality

# Make sure virtual environment is activated
source .venv/bin/activate

# Make sure package is importable
python -c "import airquality.util"
```

### Module Not Found
```bash
# Check PYTHONPATH
echo $PYTHONPATH

# Add project root to PYTHONPATH if needed
export PYTHONPATH=/home/jdowling/Projects/mlfs-book:$PYTHONPATH
```

### Fixture Not Found
- Check that fixture is defined in `conftest.py`
- Check fixture spelling matches parameter name
- Verify fixture scope is appropriate

### Mock Not Working
- Verify mock is set up before import in `conftest.py`
- Check monkeypatch is applied correctly
- Ensure mock return values match expected structure

## Best Practices

1. **Keep tests independent** - Each test should run in isolation
2. **Use descriptive names** - Test names should explain what they test
3. **Test one thing** - Each test should verify one behavior
4. **Use fixtures** - Reuse setup code with fixtures
5. **Mock external calls** - Never make real API calls in tests
6. **Assert meaningfully** - Use clear assertion messages
7. **Clean up resources** - Close files, plots, connections
8. **Document edge cases** - Explain why edge cases matter

## Coverage Goals

Target coverage: **>80%** for `airquality/util.py`

Current coverage (run `pytest --cov` to check):
- Weather API functions: ~95%
- Air quality API functions: ~95%
- Geocoding functions: ~90%
- Utility functions: ~85%
- Visualization functions: ~80%

## Contributing

When adding new functions to `airquality/util.py`:

1. Write tests first (TDD approach) or immediately after
2. Ensure >80% code coverage for new functions
3. Add relevant fixtures to `conftest.py`
4. Document edge cases in test docstrings
5. Run full test suite before committing
6. Update this README if adding new test categories

## Questions?

For questions about:
- **Test failures**: Check logs with `pytest -vv --tb=long`
- **Coverage gaps**: Run `pytest --cov --cov-report=term-missing`
- **Adding tests**: Follow patterns in existing test files
- **Mock setup**: Review `conftest.py` and mock class definitions
