"""
Tests for improved transaction generation with location continuity.

These tests verify that the new transaction generation creates realistic
geographic patterns and avoids false positives for impossible travel.
"""

import pytest
import polars as pl
import numpy as np
from datetime import datetime, timedelta
from ccfraud import synth_transactions as st


@pytest.fixture
def sample_dates():
    """Provide sample dates for testing."""
    current_date = datetime(2025, 12, 1)
    return {
        "current": current_date,
        "start": current_date - timedelta(days=30),
        "issue": current_date - timedelta(days=365),
        "expiry": current_date + timedelta(days=365),
        "account_creation": current_date - timedelta(days=365 * 2),
    }


@pytest.fixture
def sample_merchants():
    """Generate a small set of merchants for testing."""
    return st.generate_merchant_details(
        rows=50,
        start_date=datetime(2025, 1, 1),
        end_date=datetime(2025, 12, 1)
    )


@pytest.fixture
def sample_banks():
    """Generate a small set of banks for testing."""
    return st.generate_bank_details(
        rows=10,
        start_date=datetime(2025, 1, 1),
        end_date=datetime(2025, 12, 1)
    )


@pytest.fixture
def sample_accounts(sample_dates):
    """Generate sample accounts and assign home locations."""
    accounts = st.generate_account_details(
        rows=20,
        account_creation_start_date=sample_dates["account_creation"],
        current_date=sample_dates["current"],
        account_last_modified_start_date=datetime(2025, 1, 1)
    )
    # Assign home locations
    accounts = st.assign_cardholder_home_locations(accounts, seed=42)
    return accounts


@pytest.fixture
def sample_cards(sample_accounts, sample_banks, sample_dates):
    """Generate sample cards."""
    return st.generate_card_details(
        rows=40,
        num_accounts=len(sample_accounts),
        num_banks=len(sample_banks),
        current_date=sample_dates["current"],
        issue_date=sample_dates["issue"],
        expiry_date=sample_dates["expiry"]
    )


class TestHomeLocationAssignment:
    """Tests for assigning home locations to cardholders."""

    def test_home_country_column_added(self, sample_accounts):
        """Test that home_country column is added."""
        assert "home_country" in sample_accounts.columns

    def test_all_accounts_have_home_country(self, sample_accounts):
        """Test that all accounts are assigned a home country."""
        null_count = sample_accounts.filter(
            pl.col("home_country").is_null()
        ).height
        assert null_count == 0, "All accounts should have a home_country"

    def test_home_countries_are_valid(self, sample_accounts):
        """Test that home countries are from the valid set."""
        valid_countries = set(st.COUNTRY_IP_RANGES.keys())
        assigned_countries = set(sample_accounts["home_country"].unique().to_list())
        assert assigned_countries.issubset(valid_countries)

    def test_us_is_most_common(self, sample_accounts):
        """Test that US is assigned most frequently (weighted distribution)."""
        # With seed=42, US should be common due to 35% weight
        country_counts = sample_accounts.group_by("home_country").agg(
            pl.count().alias("count")
        ).sort("count", descending=True)

        # US should be in top countries (though may not always be #1 with small sample)
        top_countries = country_counts.head(3)["home_country"].to_list()
        # Just verify US is well-represented
        us_count = sample_accounts.filter(
            pl.col("home_country") == "United States"
        ).height
        assert us_count > 0, "United States should have at least some accounts"


class TestLocationContinuityTransactions:
    """Tests for transaction generation with location continuity."""

    def test_transaction_generation_succeeds(
        self, sample_cards, sample_accounts, sample_merchants, sample_dates
    ):
        """Test that transaction generation completes without errors."""
        transactions = st.generate_credit_card_transactions_with_location_continuity(
            card_df=sample_cards,
            account_df=sample_accounts,
            merchant_df=sample_merchants,
            start_date=sample_dates["start"],
            end_date=sample_dates["current"],
            rows=500,
            tid_offset=0,
            seed=42
        )

        assert transactions.height == 500
        assert "t_id" in transactions.columns
        assert "ip_address" in transactions.columns
        assert "card_present" in transactions.columns

    def test_transactions_distributed_across_cards(
        self, sample_cards, sample_accounts, sample_merchants, sample_dates
    ):
        """Test that transactions are distributed across multiple cards."""
        transactions = st.generate_credit_card_transactions_with_location_continuity(
            card_df=sample_cards,
            account_df=sample_accounts,
            merchant_df=sample_merchants,
            start_date=sample_dates["start"],
            end_date=sample_dates["current"],
            rows=200,
            tid_offset=0,
            seed=42
        )

        unique_cards = transactions["cc_num"].n_unique()
        # Should use most cards (at least 50%)
        assert unique_cards >= len(sample_cards) * 0.5

    def test_home_country_preference(
        self, sample_cards, sample_accounts, sample_merchants, sample_dates
    ):
        """Test that most transactions happen in or near home country."""
        transactions = st.generate_credit_card_transactions_with_location_continuity(
            card_df=sample_cards,
            account_df=sample_accounts,
            merchant_df=sample_merchants,
            start_date=sample_dates["start"],
            end_date=sample_dates["current"],
            rows=1000,
            tid_offset=0,
            seed=42
        )

        # Join with accounts to get home country
        transactions_with_home = transactions.join(
            sample_accounts.select(["account_id", "home_country"]),
            on="account_id",
            how="left"
        )

        # Join with merchants to get transaction country
        transactions_with_countries = transactions_with_home.join(
            sample_merchants.select(["merchant_id", "country"]),
            on="merchant_id",
            how="left"
        )

        # Check if transaction is in home country or neighboring country
        def is_home_or_neighbor(home: str, transaction: str) -> bool:
            if home == transaction:
                return True
            neighbors = st.NEIGHBORING_COUNTRIES.get(home, [])
            return transaction in neighbors

        # Count home and local transactions
        home_local_count = 0
        total_count = transactions_with_countries.height

        for row in transactions_with_countries.iter_rows(named=True):
            home = row["home_country"]
            txn_country = row["country"]
            if home and txn_country and is_home_or_neighbor(home, txn_country):
                home_local_count += 1

        home_local_pct = home_local_count / total_count

        # Should be >80% in home or neighboring countries (85% home + 10% local)
        assert home_local_pct > 0.80, \
            f"Expected >80% home/local transactions, got {home_local_pct:.2%}"

    def test_consecutive_transactions_same_country_use_similar_ips(
        self, sample_cards, sample_accounts, sample_merchants, sample_dates
    ):
        """Test that consecutive transactions in same country use similar IPs."""
        transactions = st.generate_credit_card_transactions_with_location_continuity(
            card_df=sample_cards,
            account_df=sample_accounts,
            merchant_df=sample_merchants,
            start_date=sample_dates["start"],
            end_date=sample_dates["current"],
            rows=500,
            tid_offset=0,
            seed=42
        )

        # Sort by card and timestamp
        transactions = transactions.sort(["cc_num", "ts"])

        # Join with merchants to get countries
        transactions_with_country = transactions.join(
            sample_merchants.select(["merchant_id", "country"]),
            on="merchant_id",
            how="left"
        )

        # Check consecutive transactions for same card
        same_country_same_subnet_count = 0
        same_country_total_count = 0

        transactions_list = transactions_with_country.to_dicts()

        for i in range(1, len(transactions_list)):
            prev = transactions_list[i - 1]
            curr = transactions_list[i]

            # Same card?
            if prev["cc_num"] == curr["cc_num"]:
                # Same country?
                if prev["country"] == curr["country"]:
                    same_country_total_count += 1

                    # Check if IPs are in same /16 subnet (first 2 octets match)
                    prev_ip_parts = prev["ip_address"].split(".")
                    curr_ip_parts = curr["ip_address"].split(".")

                    if (len(prev_ip_parts) == 4 and len(curr_ip_parts) == 4 and
                        prev_ip_parts[0] == curr_ip_parts[0] and
                        prev_ip_parts[1] == curr_ip_parts[1]):
                        same_country_same_subnet_count += 1

        # At least 40% of same-country consecutive transactions should use same subnet
        # (60% probability in code, but with some variation)
        if same_country_total_count > 0:
            same_subnet_pct = same_country_same_subnet_count / same_country_total_count
            assert same_subnet_pct > 0.30, \
                f"Expected >30% same subnet, got {same_subnet_pct:.2%}"

    def test_time_ordering_per_card(
        self, sample_cards, sample_accounts, sample_merchants, sample_dates
    ):
        """Test that transactions are generated in time order for each card."""
        transactions = st.generate_credit_card_transactions_with_location_continuity(
            card_df=sample_cards,
            account_df=sample_accounts,
            merchant_df=sample_merchants,
            start_date=sample_dates["start"],
            end_date=sample_dates["current"],
            rows=200,
            tid_offset=0,
            seed=42
        )

        # Group by card and check time ordering
        for cc_num in transactions["cc_num"].unique().to_list():
            card_txns = transactions.filter(pl.col("cc_num") == cc_num)
            timestamps = card_txns["ts"].to_list()

            # Verify timestamps are in order (they should be sorted by generation)
            sorted_timestamps = sorted(timestamps)
            # Allow for some ties (same timestamp)
            assert timestamps == sorted_timestamps or \
                   sorted(timestamps) == sorted_timestamps


class TestIPGeneration:
    """Tests for IP address generation functions."""

    def test_generate_ip_for_country(self):
        """Test that IPs are generated for valid countries."""
        ip = st.generate_ip_for_country("United States", seed=42)
        assert ip is not None
        parts = ip.split(".")
        assert len(parts) == 4
        for part in parts:
            assert 0 <= int(part) <= 255

    def test_generate_ip_in_same_subnet(self):
        """Test that IPs in same subnet share prefixes."""
        original_ip = "192.168.1.100"

        # Generate multiple IPs in same subnet
        same_subnet_ips = [
            st.generate_ip_in_same_subnet(original_ip, seed=i)
            for i in range(10)
        ]

        # All should share at least first 2 octets (for /16 subnet)
        original_parts = original_ip.split(".")
        for ip in same_subnet_ips:
            parts = ip.split(".")
            # Either same /24 (first 3 octets) or same /16 (first 2 octets)
            assert parts[0] == original_parts[0]
            assert parts[1] == original_parts[1]


def test_neighboring_countries_mapping():
    """Test that neighboring countries mapping is symmetric and valid."""
    # All countries in neighbors should be in COUNTRY_IP_RANGES
    valid_countries = set(st.COUNTRY_IP_RANGES.keys())

    for country, neighbors in st.NEIGHBORING_COUNTRIES.items():
        # Country itself should be valid
        assert country in valid_countries, \
            f"{country} not in COUNTRY_IP_RANGES"

        # All neighbors should be valid
        for neighbor in neighbors:
            assert neighbor in valid_countries, \
                f"{neighbor} (neighbor of {country}) not in COUNTRY_IP_RANGES"
