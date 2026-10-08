from datetime import datetime

import polars as pl

from ccfraud.features.cc_trans_fg import time_since_last_trans


def series(values):
    return pl.Series(values, dtype=pl.Datetime("us"))


class TestTimeSinceLastTrans:
    """Unit tests for time_since_last_trans (Polars)."""

    def test_basic_time_difference(self):
        ts = series([datetime(2024, 1, 1, 12, 0, 0), datetime(2024, 1, 1, 12, 5, 0), datetime(2024, 1, 1, 12, 10, 0)])
        prev_ts = series([datetime(2024, 1, 1, 11, 55, 0), datetime(2024, 1, 1, 12, 0, 0), datetime(2024, 1, 1, 12, 5, 0)])
        assert time_since_last_trans(ts, prev_ts).to_list() == [300, 300, 300]

    def test_no_previous_transaction(self):
        ts = series([datetime(2024, 1, 1, 12, 0, 0), datetime(2024, 1, 1, 12, 5, 0)])
        prev_ts = series([None, None])
        assert time_since_last_trans(ts, prev_ts).to_list() == [0, 0]

    def test_mixed_null_and_timestamps(self):
        ts = series([
            datetime(2024, 1, 1, 12, 0, 0),
            datetime(2024, 1, 1, 12, 5, 0),
            datetime(2024, 1, 1, 12, 10, 0),
            datetime(2024, 1, 1, 12, 15, 0),
        ])
        prev_ts = series([None, datetime(2024, 1, 1, 12, 0, 0), None, datetime(2024, 1, 1, 12, 10, 0)])
        assert time_since_last_trans(ts, prev_ts).to_list() == [0, 300, 0, 300]

    def test_zero_time_difference(self):
        ts = series([datetime(2024, 1, 1, 12, 0, 0), datetime(2024, 1, 1, 12, 5, 0)])
        assert time_since_last_trans(ts, ts).to_list() == [0, 0]

    def test_large_time_differences(self):
        ts = series([datetime(2024, 1, 2, 12, 0, 0), datetime(2024, 1, 1, 15, 0, 0)])
        prev_ts = series([datetime(2024, 1, 1, 12, 0, 0), datetime(2024, 1, 1, 12, 0, 0)])
        assert time_since_last_trans(ts, prev_ts).to_list() == [86400, 10800]

    def test_negative_time_difference(self):
        ts = series([datetime(2024, 1, 1, 12, 0, 0)])
        prev_ts = series([datetime(2024, 1, 1, 13, 0, 0)])
        assert time_since_last_trans(ts, prev_ts).to_list() == [-3600]

    def test_empty_series(self):
        result = time_since_last_trans(series([]), series([]))
        assert result.len() == 0
        assert result.dtype == pl.Int64

    def test_return_type(self):
        ts = series([datetime(2024, 1, 1, 12, 5, 0)])
        prev_ts = series([datetime(2024, 1, 1, 12, 0, 0)])
        assert time_since_last_trans(ts, prev_ts).dtype == pl.Int64

    def test_fractional_seconds(self):
        ts = series([datetime(2024, 1, 1, 12, 0, 0, 500000)])
        prev_ts = series([datetime(2024, 1, 1, 12, 0, 0)])
        assert time_since_last_trans(ts, prev_ts).to_list() == [0]

    def test_works_as_expression_over_cards(self):
        """The batch feature pipeline computes prev_ts per card with a shift over cc_num."""
        df = pl.DataFrame({
            "cc_num": ["A", "A", "B"],
            "ts": series([datetime(2024, 1, 1, 12, 0, 0), datetime(2024, 1, 1, 12, 1, 0), datetime(2024, 1, 1, 12, 2, 0)]),
        }).sort(["cc_num", "ts"]).with_columns(pl.col("ts").shift(1).over("cc_num").alias("prev_ts"))
        assert time_since_last_trans(df["ts"], df["prev_ts"]).to_list() == [0, 60, 0]
