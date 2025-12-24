"""
Tests for improved fraud generation patterns.

These tests verify that fraud patterns (geographic and chain attacks)
are generated with realistic characteristics that should be detectable.
"""

import pytest
import polars as pl
import numpy as np
from datetime import datetime, timedelta
from ccfraud import synth_transactions as st


@pytest.fixture
def sample_data():
    """Generate sample data for fraud testing."""
    current_date = datetime(2025, 12, 1)
    start_date = current_date - timedelta(days=30)

    # Generate entities
    merchants = st.generate_merchant_details(
        rows=100,
        start_date=datetime(2025, 1, 1),
        end_date=current_date
    )

    banks = st.generate_bank_details(
        rows=20,
        start_date=datetime(2025, 1, 1),
        end_date=current_date
    )

    accounts = st.generate_account_details(
        rows=50,
        account_creation_start_date=current_date - timedelta(days=365 * 2),
        current_date=current_date,
        account_last_modified_start_date=datetime(2025, 1, 1)
    )
    accounts = st.assign_cardholder_home_locations(accounts, seed=42)

    cards = st.generate_card_details(
        rows=100,
        num_accounts=len(accounts),
        num_banks=len(banks),
        current_date=current_date,
        issue_date=current_date - timedelta(days=365),
        expiry_date=current_date + timedelta(days=365)
    )

    # Generate legitimate transactions
    transactions = st.generate_credit_card_transactions_with_location_continuity(
        card_df=cards,
        account_df=accounts,
        merchant_df=merchants,
        start_date=start_date,
        end_date=current_date,
        rows=5000,
        tid_offset=0,
        seed=42
    )

    return {
        "transactions": transactions,
        "cards": cards,
        "merchants": merchants,
        "accounts": accounts,
        "current_date": current_date,
        "start_date": start_date
    }


class TestFraudGeneration:
    """Tests for general fraud generation."""

    def test_fraud_generation_succeeds(self, sample_data):
        """Test that fraud generation completes without errors."""
        transactions, fraud_df = st.generate_fraud(
            transaction_df=sample_data["transactions"],
            card_df=sample_data["cards"],
            merchant_df=sample_data["merchants"],
            fraud_rate=0.001,  # 0.1%
            chain_attack_ratio=0.9,
            seed=42
        )

        assert fraud_df.height > 0, "Should generate some fraud"
        assert "t_id" in fraud_df.columns
        assert "explanation" in fraud_df.columns
        assert "fraud_type" not in fraud_df.columns  # This is internal, not in final df

    def test_fraud_rate_approximately_correct(self, sample_data):
        """Test that fraud rate is approximately as specified."""
        fraud_rate = 0.001  # 0.1%

        transactions, fraud_df = st.generate_fraud(
            transaction_df=sample_data["transactions"],
            card_df=sample_data["cards"],
            merchant_df=sample_data["merchants"],
            fraud_rate=fraud_rate,
            chain_attack_ratio=0.9,
            seed=42
        )

        original_count = sample_data["transactions"].height
        fraud_count = fraud_df.height
        total_count = transactions.height

        # Fraud count should be approximately fraud_rate * original_count
        expected_fraud = int(original_count * fraud_rate)
        # Allow 50% tolerance due to rounding and attack patterns
        assert fraud_count >= expected_fraud * 0.5, \
            f"Expected ~{expected_fraud} fraud, got {fraud_count}"

        # Total should be original + fraud
        assert total_count == original_count + fraud_count

    def test_chain_attack_ratio(self, sample_data):
        """Test that chain attacks vs geographic fraud ratio is approximately correct."""
        transactions, fraud_df = st.generate_fraud(
            transaction_df=sample_data["transactions"],
            card_df=sample_data["cards"],
            merchant_df=sample_data["merchants"],
            fraud_rate=0.002,  # Higher rate to get more samples
            chain_attack_ratio=0.9,
            seed=42
        )

        # Count fraud types by explanation
        chain_count = fraud_df.filter(
            pl.col("explanation").str.contains("Chain attack")
        ).height

        geo_count = fraud_df.filter(
            pl.col("explanation").str.contains("Geographic fraud")
        ).height

        total_fraud = fraud_df.height

        # Ratio should be approximately 90% chain, 10% geographic
        chain_ratio = chain_count / total_fraud if total_fraud > 0 else 0
        # Allow some tolerance (70-95%)
        assert 0.70 <= chain_ratio <= 0.95, \
            f"Expected ~90% chain attacks, got {chain_ratio:.2%}"


class TestChainAttacks:
    """Tests for chain attack fraud patterns."""

    def test_chain_attacks_use_same_card(self, sample_data):
        """Test that chain attacks reuse the same card."""
        transactions, fraud_df = st.generate_fraud(
            transaction_df=sample_data["transactions"],
            card_df=sample_data["cards"],
            merchant_df=sample_data["merchants"],
            fraud_rate=0.002,
            chain_attack_ratio=1.0,  # All chain attacks
            seed=42
        )

        # Get fraud transactions
        fraud_txns = transactions.join(
            fraud_df.select(["t_id"]),
            on="t_id",
            how="inner"
        ).sort(["cc_num", "ts"])

        # Check that some cards have multiple fraud transactions
        fraud_per_card = fraud_txns.group_by("cc_num").agg(
            pl.count().alias("fraud_count")
        )

        cards_with_multiple_fraud = fraud_per_card.filter(
            pl.col("fraud_count") >= 5
        ).height

        # Should have some chains (5-15 transactions per attack)
        assert cards_with_multiple_fraud > 0, \
            "Chain attacks should have multiple transactions per card"

    def test_chain_attacks_have_small_amounts(self, sample_data):
        """Test that chain attack amounts are small (<$50)."""
        transactions, fraud_df = st.generate_fraud(
            transaction_df=sample_data["transactions"],
            card_df=sample_data["cards"],
            merchant_df=sample_data["merchants"],
            fraud_rate=0.002,
            chain_attack_ratio=1.0,  # All chain attacks
            seed=42
        )

        # Get fraud transactions
        fraud_txns = transactions.join(
            fraud_df.select(["t_id"]),
            on="t_id",
            how="inner"
        )

        chain_fraud = fraud_txns.join(
            fraud_df.filter(pl.col("explanation").str.contains("Chain attack")),
            on="t_id",
            how="inner"
        )

        # All chain attack amounts should be < $50
        max_amount = chain_fraud["amount"].max()
        assert max_amount < 50.0, \
            f"Chain attack amounts should be < $50, got max {max_amount}"

    def test_chain_attacks_not_card_present(self, sample_data):
        """Test that chain attacks are online transactions (card_present=False)."""
        transactions, fraud_df = st.generate_fraud(
            transaction_df=sample_data["transactions"],
            card_df=sample_data["cards"],
            merchant_df=sample_data["merchants"],
            fraud_rate=0.002,
            chain_attack_ratio=1.0,  # All chain attacks
            seed=42
        )

        # Get fraud transactions
        fraud_txns = transactions.join(
            fraud_df.select(["t_id"]),
            on="t_id",
            how="inner"
        )

        # All chain attacks should be online (card_present=False)
        card_present_count = fraud_txns.filter(
            pl.col("card_present") == True
        ).height

        assert card_present_count == 0, \
            "Chain attacks should all be online (card_present=False)"

    def test_chain_attacks_have_varied_amounts(self, sample_data):
        """Test that chain attacks start small and increase."""
        transactions, fraud_df = st.generate_fraud(
            transaction_df=sample_data["transactions"],
            card_df=sample_data["cards"],
            merchant_df=sample_data["merchants"],
            fraud_rate=0.003,  # Higher rate to get more chains
            chain_attack_ratio=1.0,  # All chain attacks
            seed=42
        )

        # Get fraud transactions sorted by card and time
        fraud_txns = transactions.join(
            fraud_df.select(["t_id"]),
            on="t_id",
            how="inner"
        ).sort(["cc_num", "ts"])

        # For each card with multiple transactions, check amount progression
        fraud_per_card = fraud_txns.group_by("cc_num").agg(
            pl.count().alias("fraud_count")
        ).filter(pl.col("fraud_count") >= 3)

        found_progression = False

        for card in fraud_per_card["cc_num"].to_list():
            card_txns = fraud_txns.filter(pl.col("cc_num") == card)
            amounts = card_txns["amount"].to_list()

            # Check if first transaction is smaller than average
            if len(amounts) >= 3:
                first_amount = amounts[0]
                avg_amount = sum(amounts) / len(amounts)

                if first_amount < avg_amount * 0.7:  # First is <70% of average
                    found_progression = True
                    break

        assert found_progression, \
            "Chain attacks should show amount progression (small to larger)"


class TestGeographicFraud:
    """Tests for geographic fraud patterns."""

    def test_geographic_fraud_uses_distant_countries(self, sample_data):
        """Test that geographic fraud involves distant countries."""
        transactions, fraud_df = st.generate_fraud(
            transaction_df=sample_data["transactions"],
            card_df=sample_data["cards"],
            merchant_df=sample_data["merchants"],
            fraud_rate=0.002,
            chain_attack_ratio=0.0,  # All geographic fraud
            seed=42
        )

        # Get geographic fraud transactions
        geo_fraud_ids = fraud_df.filter(
            pl.col("explanation").str.contains("Geographic fraud")
        )["t_id"].to_list()

        geo_txns = transactions.filter(
            pl.col("t_id").is_in(geo_fraud_ids)
        ).sort(["cc_num", "ts"])

        # Join with merchants to get countries
        geo_txns_with_country = geo_txns.join(
            sample_data["merchants"].select(["merchant_id", "country"]),
            on="merchant_id",
            how="left"
        )

        # Check consecutive transactions for same card
        txns_list = geo_txns_with_country.to_dicts()

        found_distant_pair = False
        for i in range(1, len(txns_list)):
            if txns_list[i]["cc_num"] == txns_list[i-1]["cc_num"]:
                country1 = txns_list[i-1]["country"]
                country2 = txns_list[i]["country"]

                # Should be different countries
                if country1 != country2:
                    # And not neighbors
                    neighbors = st.NEIGHBORING_COUNTRIES.get(country1, [])
                    if country2 not in neighbors:
                        found_distant_pair = True
                        break

        assert found_distant_pair, \
            "Geographic fraud should involve distant (non-neighboring) countries"

    def test_geographic_fraud_has_short_time_gaps(self, sample_data):
        """Test that geographic fraud has impossibly short time gaps."""
        transactions, fraud_df = st.generate_fraud(
            transaction_df=sample_data["transactions"],
            card_df=sample_data["cards"],
            merchant_df=sample_data["merchants"],
            fraud_rate=0.002,
            chain_attack_ratio=0.0,  # All geographic fraud
            seed=42
        )

        # Get geographic fraud transactions
        geo_fraud_ids = fraud_df.filter(
            pl.col("explanation").str.contains("Geographic fraud")
        )["t_id"].to_list()

        geo_txns = transactions.filter(
            pl.col("t_id").is_in(geo_fraud_ids)
        ).sort(["cc_num", "ts"])

        # Check time gaps between consecutive transactions for same card
        txns_list = geo_txns.to_dicts()

        time_gaps = []
        for i in range(1, len(txns_list)):
            if txns_list[i]["cc_num"] == txns_list[i-1]["cc_num"]:
                gap_seconds = (txns_list[i]["ts"] - txns_list[i-1]["ts"]).total_seconds()
                time_gaps.append(gap_seconds)

        # All gaps should be <= 1 hour (3600 seconds)
        if time_gaps:
            max_gap = max(time_gaps)
            assert max_gap <= 3600, \
                f"Geographic fraud time gaps should be <=1 hour, got {max_gap/60:.1f} min"

            # Some should be very short (< 30 min)
            short_gaps = [g for g in time_gaps if g <= 1800]
            assert len(short_gaps) > 0, \
                "Some geographic fraud should have very short time gaps (<30 min)"

    def test_geographic_fraud_is_card_present(self, sample_data):
        """Test that geographic fraud uses physical cards."""
        transactions, fraud_df = st.generate_fraud(
            transaction_df=sample_data["transactions"],
            card_df=sample_data["cards"],
            merchant_df=sample_data["merchants"],
            fraud_rate=0.002,
            chain_attack_ratio=0.0,  # All geographic fraud
            seed=42
        )

        # Get geographic fraud transactions
        geo_fraud_ids = fraud_df.filter(
            pl.col("explanation").str.contains("Geographic fraud")
        )["t_id"].to_list()

        geo_txns = transactions.filter(
            pl.col("t_id").is_in(geo_fraud_ids)
        )

        # All should be card present
        card_present_count = geo_txns.filter(
            pl.col("card_present") == True
        ).height

        assert card_present_count == geo_txns.height, \
            "Geographic fraud should all be card-present transactions"

    def test_geographic_fraud_comes_in_pairs(self, sample_data):
        """Test that geographic fraud generates pairs of transactions."""
        transactions, fraud_df = st.generate_fraud(
            transaction_df=sample_data["transactions"],
            card_df=sample_data["cards"],
            merchant_df=sample_data["merchants"],
            fraud_rate=0.002,
            chain_attack_ratio=0.0,  # All geographic fraud
            seed=42
        )

        # Get geographic fraud grouped by card
        geo_fraud_ids = fraud_df.filter(
            pl.col("explanation").str.contains("Geographic fraud")
        )["t_id"].to_list()

        geo_txns = transactions.filter(
            pl.col("t_id").is_in(geo_fraud_ids)
        )

        fraud_per_card = geo_txns.group_by("cc_num").agg(
            pl.count().alias("fraud_count")
        )

        # Most cards should have exactly 2 fraud transactions (a pair)
        pairs = fraud_per_card.filter(pl.col("fraud_count") == 2).height
        total_cards = fraud_per_card.height

        if total_cards > 0:
            pair_ratio = pairs / total_cards
            # At least 50% should be pairs
            assert pair_ratio >= 0.5, \
                f"Expected most geographic fraud in pairs, got {pair_ratio:.2%}"


def test_fraud_explanations_are_informative(sample_data):
    """Test that fraud explanations contain useful information."""
    transactions, fraud_df = st.generate_fraud(
        transaction_df=sample_data["transactions"],
        card_df=sample_data["cards"],
        merchant_df=sample_data["merchants"],
        fraud_rate=0.001,
        chain_attack_ratio=0.5,
        seed=42
    )

    # All fraud should have non-empty explanations
    empty_explanations = fraud_df.filter(
        pl.col("explanation").is_null() | (pl.col("explanation") == "")
    ).height

    assert empty_explanations == 0, "All fraud should have explanations"

    # Chain attacks should mention "Chain attack"
    chain_fraud = fraud_df.filter(
        pl.col("explanation").str.contains("Chain attack")
    )
    assert chain_fraud.height > 0, "Should have chain attack explanations"

    # Geographic fraud should mention "Geographic fraud"
    geo_fraud = fraud_df.filter(
        pl.col("explanation").str.contains("Geographic fraud")
    )
    assert geo_fraud.height > 0, "Should have geographic fraud explanations"
