import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from ccfraud.features.cc_trans_fg import time_since_last_trans


class TestTimeSinceLastTrans:
    """Unit tests for time_since_last_trans function."""

    def test_basic_time_difference(self):
        """Test basic time difference calculation between transactions."""
        ts = pd.Series([
            datetime(2024, 1, 1, 12, 0, 0),
            datetime(2024, 1, 1, 12, 5, 0),
            datetime(2024, 1, 1, 12, 10, 0),
        ])
        prev_ts = pd.Series([
            datetime(2024, 1, 1, 11, 55, 0),
            datetime(2024, 1, 1, 12, 0, 0),
            datetime(2024, 1, 1, 12, 5, 0),
        ])

        result = time_since_last_trans(ts, prev_ts)

        expected = pd.Series([300, 300, 300], dtype=int)  # 5 minutes = 300 seconds
        pd.testing.assert_series_equal(result, expected)

    def test_no_previous_transaction(self):
        """Test that NaT/None previous transactions return 0."""
        ts = pd.Series([
            datetime(2024, 1, 1, 12, 0, 0),
            datetime(2024, 1, 1, 12, 5, 0),
            datetime(2024, 1, 1, 12, 10, 0),
        ])
        prev_ts = pd.Series([pd.NaT, pd.NaT, pd.NaT])

        result = time_since_last_trans(ts, prev_ts)

        expected = pd.Series([0, 0, 0], dtype=int)
        pd.testing.assert_series_equal(result, expected)

    def test_mixed_nat_and_timestamps(self):
        """Test handling of mixed NaT and valid timestamps."""
        ts = pd.Series([
            datetime(2024, 1, 1, 12, 0, 0),
            datetime(2024, 1, 1, 12, 5, 0),
            datetime(2024, 1, 1, 12, 10, 0),
            datetime(2024, 1, 1, 12, 15, 0),
        ])
        prev_ts = pd.Series([
            pd.NaT,
            datetime(2024, 1, 1, 12, 0, 0),
            pd.NaT,
            datetime(2024, 1, 1, 12, 10, 0),
        ])

        result = time_since_last_trans(ts, prev_ts)

        expected = pd.Series([0, 300, 0, 300], dtype=int)
        pd.testing.assert_series_equal(result, expected)

    def test_zero_time_difference(self):
        """Test when current and previous transaction times are identical."""
        ts = pd.Series([
            datetime(2024, 1, 1, 12, 0, 0),
            datetime(2024, 1, 1, 12, 5, 0),
        ])
        prev_ts = pd.Series([
            datetime(2024, 1, 1, 12, 0, 0),
            datetime(2024, 1, 1, 12, 5, 0),
        ])

        result = time_since_last_trans(ts, prev_ts)

        expected = pd.Series([0, 0], dtype=int)
        pd.testing.assert_series_equal(result, expected)

    def test_large_time_differences(self):
        """Test calculation with large time differences (hours/days)."""
        ts = pd.Series([
            datetime(2024, 1, 2, 12, 0, 0),  # 1 day later
            datetime(2024, 1, 1, 15, 0, 0),  # 3 hours later
        ])
        prev_ts = pd.Series([
            datetime(2024, 1, 1, 12, 0, 0),
            datetime(2024, 1, 1, 12, 0, 0),
        ])

        result = time_since_last_trans(ts, prev_ts)

        expected = pd.Series([86400, 10800], dtype=int)  # 1 day = 86400s, 3 hours = 10800s
        pd.testing.assert_series_equal(result, expected)

    def test_negative_time_difference(self):
        """Test that negative time differences work (though unusual in practice)."""
        ts = pd.Series([
            datetime(2024, 1, 1, 12, 0, 0),
        ])
        prev_ts = pd.Series([
            datetime(2024, 1, 1, 13, 0, 0),  # Previous is in the future
        ])

        result = time_since_last_trans(ts, prev_ts)

        expected = pd.Series([-3600], dtype=int)  # -1 hour
        pd.testing.assert_series_equal(result, expected)

    def test_empty_series(self):
        """Test handling of empty Series."""
        ts = pd.Series([], dtype='datetime64[ns]')
        prev_ts = pd.Series([], dtype='datetime64[ns]')

        result = time_since_last_trans(ts, prev_ts)

        expected = pd.Series([], dtype=int)
        pd.testing.assert_series_equal(result, expected)

    def test_return_type(self):
        """Test that return type is integer."""
        ts = pd.Series([datetime(2024, 1, 1, 12, 5, 0)])
        prev_ts = pd.Series([datetime(2024, 1, 1, 12, 0, 0)])

        result = time_since_last_trans(ts, prev_ts)

        assert result.dtype == np.int64 or result.dtype == np.int32

    def test_fractional_seconds(self):
        """Test calculation with microseconds precision."""
        ts = pd.Series([
            datetime(2024, 1, 1, 12, 0, 0, 500000),  # .5 seconds
        ])
        prev_ts = pd.Series([
            datetime(2024, 1, 1, 12, 0, 0),
        ])

        result = time_since_last_trans(ts, prev_ts)

        # Should truncate to 0 seconds since we convert to int
        expected = pd.Series([0], dtype=int)
        pd.testing.assert_series_equal(result, expected)
