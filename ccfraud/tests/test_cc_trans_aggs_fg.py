"""Unit tests for the Polars sliding-window aggregates (features/cc_trans_aggs_fg.py)."""

from datetime import datetime, timedelta

import polars as pl
import pytest

from ccfraud.features import cc_trans_aggs_fg as aggs

T0 = datetime(2026, 1, 1, 12, 0, 0)


def txns(rows):
    """rows: (cc_num, ts, amount, ip, card_present)"""
    return pl.DataFrame(
        {
            "cc_num": [r[0] for r in rows],
            "account_id": [f"ACC_{r[0]}" for r in rows],
            "amount": [float(r[2]) for r in rows],
            "ip_address": [r[3] for r in rows],
            "card_present": [r[4] for r in rows],
            "ts": [r[1] for r in rows],
        }
    )


def window(df, cc_num, end):
    rows = df.filter((pl.col("cc_num") == cc_num) & (pl.col("event_time") == end))
    assert rows.height == 1, f"expected one window for {cc_num} ending {end}, got {rows.height}"
    return rows.row(0, named=True)


class TestWindowAggregates:
    def test_single_transaction_is_in_sixty_windows(self):
        df = aggs.window_aggregates(txns([("A", T0 + timedelta(seconds=30), 10, "1.1.1.1", False)]))
        # windows [end-60m, end) containing 12:00:30 end at 12:01 ... 13:00
        assert df.height == 60
        ends = df["event_time"].to_list()
        assert min(ends) == T0 + timedelta(minutes=1)
        assert max(ends) == T0 + timedelta(minutes=60)
        assert all(e.second == 0 and e.microsecond == 0 for e in ends)

    def test_hour_and_short_window_features(self):
        df = aggs.window_aggregates(
            txns(
                [
                    ("A", T0 + timedelta(minutes=5), 10, "1.1.1.1", False),
                    ("A", T0 + timedelta(minutes=20), 30, "2.2.2.2", True),
                    ("A", T0 + timedelta(minutes=25), 20, "1.1.1.1", False),
                ]
            )
        )
        # Window [11:30, 12:30): all three transactions, the last two in its final 10 minutes
        w = window(df, "A", T0 + timedelta(minutes=30))
        assert w["num_trans_last_hour"] == 3
        assert w["sum_trans_last_hour"] == 60.0
        assert w["max_trans_last_hour"] == 30.0
        assert w["num_ip_addresses_last_hour"] == 2
        assert w["num_trans_last_10_mins"] == 2
        assert w["sum_trans_last_10_mins"] == 50.0
        assert w["prev_ts"] == T0 + timedelta(minutes=25)
        assert w["prev_ip_address"] == "1.1.1.1"
        assert w["prev_card_present"] is False
        assert w["account_id"] == "ACC_A"
        # Window [11:50, 12:50): same hour aggregates, nothing in its last 10 minutes
        w = window(df, "A", T0 + timedelta(minutes=50))
        assert w["num_trans_last_hour"] == 3
        assert w["num_trans_last_10_mins"] == 0
        assert w["sum_trans_last_10_mins"] == 0.0
        # Window [12:06, 13:06): the first transaction has dropped out
        w = window(df, "A", T0 + timedelta(minutes=66))
        assert w["num_trans_last_hour"] == 2
        assert w["sum_trans_last_hour"] == 50.0

    def test_window_boundaries_are_closed_left_open_right(self):
        df = aggs.window_aggregates(txns([("A", T0, 10, "1.1.1.1", False)]))
        ends = set(df["event_time"].to_list())
        assert T0 not in ends  # [11:00, 12:00) does not contain 12:00
        assert T0 + timedelta(minutes=1) in ends  # [11:01, 12:01) does
        assert T0 + timedelta(minutes=60) in ends  # [12:00, 13:00) does

    def test_cards_are_independent_and_empty_windows_are_omitted(self):
        df = aggs.window_aggregates(
            txns(
                [
                    ("A", T0, 10, "1.1.1.1", False),
                    ("B", T0 + timedelta(hours=5), 99, "9.9.9.9", True),
                ]
            )
        )
        assert df.filter(pl.col("cc_num") == "A").height == 60
        assert df.filter(pl.col("cc_num") == "B").height == 60
        assert df.filter(pl.col("num_trans_last_hour") == 0).height == 0
        assert window(df, "B", T0 + timedelta(hours=6))["max_trans_last_hour"] == 99.0

    def test_output_columns(self):
        df = aggs.window_aggregates(txns([("A", T0, 10, "1.1.1.1", False)]))
        assert set(df.columns) == set(aggs.AGGS_COLUMNS) - {"bank_id"}
        assert df.schema["event_time"] == pl.Datetime("us")
        assert df.schema["num_trans_last_hour"] == pl.Int64
        assert df.schema["num_trans_last_10_mins"] == pl.Int64


class TestLookupWindows:
    def test_selects_latest_closed_window_before_each_probe(self):
        windows = aggs.window_aggregates(
            txns(
                [
                    ("A", T0 + timedelta(minutes=5), 10, "1.1.1.1", False),
                    ("A", T0 + timedelta(minutes=30, seconds=20), 30, "2.2.2.2", True),
                ]
            )
        )
        probes = pl.DataFrame(
            {
                "cc_num": ["A", "A", "A", "A"],
                "ts": [
                    T0 + timedelta(minutes=5),  # the first transaction itself: nothing before it
                    T0 + timedelta(minutes=30, seconds=20),  # second transaction: window ending 12:30
                    T0 + timedelta(minutes=31),  # exactly on a window end: that window
                    T0 + timedelta(hours=3),  # long after: the last non-empty window, ending 13:30
                ],
            }
        )
        got = aggs.lookup_windows(windows, probes).sort("ts")
        assert got.height == 3  # the first probe has no window before it
        assert got["event_time"].to_list() == [
            T0 + timedelta(minutes=30),
            T0 + timedelta(minutes=31),
            T0 + timedelta(minutes=90),
        ]
        # the window selected for the second transaction does not contain it
        assert got.row(0, named=True)["num_trans_last_hour"] == 1
        assert got.row(0, named=True)["prev_ts"] == T0 + timedelta(minutes=5)
        # the window ending exactly at the probe time contains the second transaction
        assert got.row(1, named=True)["num_trans_last_hour"] == 2
        assert got.row(2, named=True)["num_trans_last_hour"] == 1
        assert got.row(2, named=True)["prev_ts"] == T0 + timedelta(minutes=30, seconds=20)

    def test_probes_are_matched_per_card(self):
        windows = aggs.window_aggregates(
            txns(
                [
                    ("A", T0, 10, "1.1.1.1", False),
                    ("B", T0 + timedelta(minutes=10), 20, "2.2.2.2", False),
                ]
            )
        )
        probes = pl.DataFrame({"cc_num": ["B", "A"], "ts": [T0 + timedelta(minutes=5), T0 + timedelta(minutes=5)]})
        got = aggs.lookup_windows(windows, probes)
        assert got["cc_num"].to_list() == ["A"]  # B has no window yet
        assert got["max_trans_last_hour"].to_list() == [10.0]


class TestWithBankIds:
    def test_latest_card_row_wins_and_columns_are_ordered(self):
        windows = aggs.window_aggregates(txns([("A", T0, 10, "1.1.1.1", False)])).head(1)
        cards = pl.DataFrame(
            {
                "cc_num": ["A", "A", "B"],
                "bank_id": ["BANK_old", "BANK_new", "BANK_B"],
                "last_modified": [T0 - timedelta(days=2), T0 - timedelta(days=1), T0],
            }
        )
        got = aggs.with_bank_ids(windows, cards)
        assert got.columns == aggs.AGGS_COLUMNS
        assert got["bank_id"].to_list() == ["BANK_new"]


@pytest.mark.parametrize("second,micro", [(0, 0), (59, 999999)])
def test_floor_minute(second, micro):
    assert aggs.floor_minute(T0.replace(second=second, microsecond=micro)) == T0
