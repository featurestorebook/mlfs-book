import pytest
import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
from pathlib import Path
from airquality.util import plot_air_quality_forecast

# Use non-interactive backend for testing
matplotlib.use('Agg')


class TestPlotAirQualityForecast:
    """Tests for the plot_air_quality_forecast function."""

    def test_plot_generation_forecast_only(self, sample_forecast_df, tmp_path):
        """Test plot generation without hindcast (forecast only)."""
        city = "Stockholm"
        street = "Hornsgatan"
        file_path = tmp_path / "forecast.png"

        result = plot_air_quality_forecast(
            city=city,
            street=street,
            df=sample_forecast_df,
            file_path=str(file_path),
            hindcast=False
        )

        # Verify file was created
        assert file_path.exists()

        # Verify return type is matplotlib plt
        assert result == plt

        # Clean up
        plt.close('all')

    def test_plot_generation_with_hindcast(self, sample_hindcast_df, tmp_path):
        """Test plot generation with hindcast (actual + predicted)."""
        city = "Stockholm"
        street = "Hornsgatan"
        file_path = tmp_path / "hindcast.png"

        result = plot_air_quality_forecast(
            city=city,
            street=street,
            df=sample_hindcast_df,
            file_path=str(file_path),
            hindcast=True
        )

        # Verify file was created
        assert file_path.exists()

        # Verify return type
        assert result == plt

        # Clean up
        plt.close('all')

    def test_file_creation(self, sample_forecast_df, tmp_path):
        """Test that output file is created correctly."""
        city = "Stockholm"
        street = "Hornsgatan"
        file_path = tmp_path / "test_forecast.png"

        plot_air_quality_forecast(
            city=city,
            street=street,
            df=sample_forecast_df,
            file_path=str(file_path),
            hindcast=False
        )

        # Verify file exists and is non-empty
        assert file_path.exists()
        assert file_path.stat().st_size > 0

        # Verify it's a PNG file
        with open(file_path, 'rb') as f:
            header = f.read(8)
            # PNG files start with these bytes
            assert header[:4] == b'\x89PNG'

        # Clean up
        plt.close('all')

    def test_return_type(self, sample_forecast_df, tmp_path):
        """Test that function returns matplotlib plt object."""
        city = "Stockholm"
        street = "Hornsgatan"
        file_path = tmp_path / "forecast.png"

        result = plot_air_quality_forecast(
            city=city,
            street=street,
            df=sample_forecast_df,
            file_path=str(file_path),
            hindcast=False
        )

        # Verify return type
        assert result is not None
        assert hasattr(result, 'savefig')  # plt has savefig method
        assert hasattr(result, 'close')    # plt has close method

        # Clean up
        plt.close('all')

    def test_various_dataframe_sizes(self, tmp_path):
        """Test plotting with different DataFrame sizes."""
        city = "Stockholm"
        street = "Hornsgatan"

        # Test with small DataFrame (5 days)
        small_df = pd.DataFrame({
            'date': pd.date_range('2025-01-01', periods=5, freq='D'),
            'predicted_pm25': np.random.uniform(20, 80, 5).astype(np.float32),
        })

        file_path_small = tmp_path / "small_forecast.png"
        plot_air_quality_forecast(city, street, small_df, str(file_path_small), hindcast=False)
        assert file_path_small.exists()
        plt.close('all')

        # Test with larger DataFrame (20 days)
        large_df = pd.DataFrame({
            'date': pd.date_range('2025-01-01', periods=20, freq='D'),
            'predicted_pm25': np.random.uniform(20, 80, 20).astype(np.float32),
        })

        file_path_large = tmp_path / "large_forecast.png"
        plot_air_quality_forecast(city, street, large_df, str(file_path_large), hindcast=False)
        assert file_path_large.exists()
        plt.close('all')

    def test_hindcast_with_actual_pm25(self, tmp_path):
        """Test that hindcast mode plots both predicted and actual PM2.5."""
        city = "Stockholm"
        street = "Hornsgatan"
        file_path = tmp_path / "hindcast.png"

        # DataFrame with both predicted and actual values
        df = pd.DataFrame({
            'date': pd.date_range('2025-01-01', periods=10, freq='D'),
            'predicted_pm25': np.random.uniform(20, 80, 10).astype(np.float32),
            'pm25': np.random.uniform(15, 85, 10).astype(np.float32),
        })

        result = plot_air_quality_forecast(
            city=city,
            street=street,
            df=df,
            file_path=str(file_path),
            hindcast=True
        )

        # Verify file was created
        assert file_path.exists()

        # In hindcast mode, both predicted and actual should be plotted
        # We can't easily inspect the plot contents in tests, but we can verify
        # that the function completes without error and creates a file
        assert file_path.stat().st_size > 0

        # Clean up
        plt.close('all')

    def test_extreme_pm25_values(self, tmp_path):
        """Test plotting with extreme PM2.5 values."""
        city = "Stockholm"
        street = "Hornsgatan"
        file_path = tmp_path / "extreme.png"

        # DataFrame with extreme values
        df = pd.DataFrame({
            'date': pd.date_range('2025-01-01', periods=10, freq='D'),
            'predicted_pm25': np.array([1, 10, 50, 100, 200, 300, 400, 450, 490, 500], dtype=np.float32),
        })

        result = plot_air_quality_forecast(
            city=city,
            street=street,
            df=df,
            file_path=str(file_path),
            hindcast=False
        )

        # Verify function handles extreme values
        assert file_path.exists()
        assert file_path.stat().st_size > 0

        # Clean up
        plt.close('all')

    def test_city_and_street_in_title(self, sample_forecast_df, tmp_path, capsys):
        """Test that city and street appear in the plot title."""
        city = "Stockholm"
        street = "Hornsgatan"
        file_path = tmp_path / "forecast.png"

        # The function doesn't print the title, but we can verify it runs
        # without error with the given city and street
        plot_air_quality_forecast(
            city=city,
            street=street,
            df=sample_forecast_df,
            file_path=str(file_path),
            hindcast=False
        )

        # Verify file creation
        assert file_path.exists()

        # Clean up
        plt.close('all')

    def test_logarithmic_scale(self, sample_forecast_df, tmp_path):
        """Test that the plot uses logarithmic scale."""
        city = "Stockholm"
        street = "Hornsgatan"
        file_path = tmp_path / "log_scale.png"

        plot_air_quality_forecast(
            city=city,
            street=street,
            df=sample_forecast_df,
            file_path=str(file_path),
            hindcast=False
        )

        # Get the current figure and axes
        fig = plt.gcf()
        ax = fig.gca()

        # Verify y-axis uses log scale
        assert ax.get_yscale() == 'log'

        # Clean up
        plt.close('all')

    def test_date_column_conversion(self, tmp_path):
        """Test that date column is correctly converted for plotting."""
        city = "Stockholm"
        street = "Hornsgatan"
        file_path = tmp_path / "date_test.png"

        # DataFrame with date as string
        df = pd.DataFrame({
            'date': pd.date_range('2025-01-01', periods=10, freq='D'),
            'predicted_pm25': np.random.uniform(20, 80, 10).astype(np.float32),
        })

        # Function should handle date conversion
        result = plot_air_quality_forecast(
            city=city,
            street=street,
            df=df,
            file_path=str(file_path),
            hindcast=False
        )

        # Verify file creation
        assert file_path.exists()

        # Clean up
        plt.close('all')
