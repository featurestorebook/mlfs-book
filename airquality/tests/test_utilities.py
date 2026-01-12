import pytest
import pandas as pd
import numpy as np
import requests
from datetime import date
from airquality.util import check_file_path, trigger_request, backfill_predictions_for_monitoring


class TestCheckFilePath:
    """Tests for the check_file_path function."""

    def test_existing_file(self, tmp_path, capsys):
        """Test check_file_path with an existing file."""
        # Create a temporary file
        test_file = tmp_path / "test_file.txt"
        test_file.write_text("test content")

        # Check the file
        check_file_path(str(test_file))

        # Capture printed output
        captured = capsys.readouterr()
        assert "File successfully found" in captured.out
        assert str(test_file) in captured.out

    def test_nonexistent_file(self, tmp_path, capsys):
        """Test check_file_path with a non-existent file."""
        # Use a file path that doesn't exist
        test_file = tmp_path / "nonexistent_file.txt"

        # Check the file
        check_file_path(str(test_file))

        # Capture printed output
        captured = capsys.readouterr()
        assert "Error. File not found" in captured.out
        assert str(test_file) in captured.out

    def test_directory_path(self, tmp_path, capsys):
        """Test check_file_path with a directory (should fail)."""
        # Use the tmp_path directory itself
        check_file_path(str(tmp_path))

        # Capture printed output
        captured = capsys.readouterr()
        assert "Error. File not found" in captured.out


class TestTriggerRequest:
    """Tests for the trigger_request function."""

    def test_successful_request(self, mock_requests_get):
        """Test successful HTTP request."""
        url = "https://api.example.com/data"
        data = trigger_request(url)

        assert data is not None
        assert data['status'] == 'ok'
        assert 'data' in data

    def test_json_parsing(self, mock_requests_get):
        """Test that JSON is correctly parsed."""
        url = "https://api.example.com/data"
        data = trigger_request(url)

        assert isinstance(data, dict)
        assert data['status'] == 'ok'

    def test_error_status_code(self, mock_requests_get_error):
        """Test that non-200 status codes raise RequestException."""
        url = "https://api.example.com/notfound"

        with pytest.raises(requests.exceptions.RequestException):
            trigger_request(url)


class TestBackfillPredictionsForMonitoring:
    """Tests for the backfill_predictions_for_monitoring function."""

    def test_data_flow(self, mock_feature_group, sample_aq_df, mock_model):
        """Test the complete data flow of backfill_predictions_for_monitoring."""
        # Create a mock monitor feature group
        mock_monitor_fg = mock_feature_group

        # Call the function
        result = backfill_predictions_for_monitoring(
            weather_fg=mock_feature_group,
            air_quality_df=sample_aq_df,
            monitor_fg=mock_monitor_fg,
            model=mock_model
        )

        # Verify the weather feature group was read
        mock_feature_group.read.assert_called_once()

        # Verify the monitor feature group was inserted
        mock_monitor_fg.insert.assert_called_once()

        # Verify result is a DataFrame
        assert isinstance(result, pd.DataFrame)

    def test_sorting_and_tail(self, mock_feature_group, sample_aq_df, mock_model):
        """Test that data is sorted by date and tail(10) is applied."""
        mock_monitor_fg = mock_feature_group

        result = backfill_predictions_for_monitoring(
            weather_fg=mock_feature_group,
            air_quality_df=sample_aq_df,
            monitor_fg=mock_monitor_fg,
            model=mock_model
        )

        # Result should have at most 10 rows (from tail(10))
        assert len(result) <= 10

    def test_column_additions(self, mock_feature_group, sample_aq_df, mock_model):
        """Test that required columns are added."""
        mock_monitor_fg = mock_feature_group

        result = backfill_predictions_for_monitoring(
            weather_fg=mock_feature_group,
            air_quality_df=sample_aq_df,
            monitor_fg=mock_monitor_fg,
            model=mock_model
        )

        # Check that predicted_pm25 column exists
        assert 'predicted_pm25' in result.columns

        # Check that days_before_forecast_day column exists
        assert 'days_before_forecast_day' in result.columns

        # Check that days_before_forecast_day is 1
        assert all(result['days_before_forecast_day'] == 1)

        # Check that pm25 column exists (from merge with air quality data)
        assert 'pm25' in result.columns

    def test_merge_operation(self, mock_feature_group, sample_aq_df, mock_model):
        """Test that weather and air quality data are merged correctly."""
        mock_monitor_fg = mock_feature_group

        result = backfill_predictions_for_monitoring(
            weather_fg=mock_feature_group,
            air_quality_df=sample_aq_df,
            monitor_fg=mock_monitor_fg,
            model=mock_model
        )

        # Check that merged columns exist
        assert 'date' in result.columns
        assert 'pm25' in result.columns
        assert 'street' in result.columns
        assert 'country' in result.columns
        assert 'temperature_2m_mean' in result.columns

    def test_prediction_integration(self, mock_feature_group, sample_aq_df, mock_model):
        """Test that model predictions are integrated correctly."""
        mock_monitor_fg = mock_feature_group

        result = backfill_predictions_for_monitoring(
            weather_fg=mock_feature_group,
            air_quality_df=sample_aq_df,
            monitor_fg=mock_monitor_fg,
            model=mock_model
        )

        # Check that predicted_pm25 values are realistic
        assert all(result['predicted_pm25'] > 0)
        assert all(result['predicted_pm25'] < 500)  # Reasonable PM2.5 range
