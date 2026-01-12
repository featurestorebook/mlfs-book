import pytest
from airquality.util import get_city_coordinates


class TestGetCityCoordinates:
    """Tests for the get_city_coordinates function."""

    def test_successful_geocoding_stockholm(self, mock_geocoder):
        """Test successful geocoding for Stockholm."""
        lat, lon = get_city_coordinates('Stockholm')
        assert lat == 59.33
        assert lon == 18.07

    def test_successful_geocoding_london(self, mock_geocoder):
        """Test successful geocoding for London."""
        lat, lon = get_city_coordinates('London')
        assert lat == 51.51
        assert lon == -0.13

    def test_successful_geocoding_new_york(self, mock_geocoder):
        """Test successful geocoding for New York."""
        lat, lon = get_city_coordinates('New York')
        assert lat == 40.71
        assert lon == -74.01

    def test_coordinate_rounding(self, mock_geocoder):
        """Test that coordinates are rounded to 2 decimal places."""
        lat, lon = get_city_coordinates('Paris')
        # Check that we get exactly 2 decimal places
        assert lat == 48.86
        assert lon == 2.35

    def test_return_type(self, mock_geocoder):
        """Test that function returns a tuple of two floats."""
        result = get_city_coordinates('Stockholm')
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], float)
        assert isinstance(result[1], float)

    @pytest.mark.parametrize("city,expected_lat,expected_lon", [
        ("Stockholm", 59.33, 18.07),
        ("London", 51.51, -0.13),
        ("New York", 40.71, -74.01),
        ("Paris", 48.86, 2.35),
    ])
    def test_multiple_cities(self, city, expected_lat, expected_lon, mock_geocoder):
        """Test geocoding for multiple cities using parametrize."""
        lat, lon = get_city_coordinates(city)
        assert lat == expected_lat
        assert lon == expected_lon
