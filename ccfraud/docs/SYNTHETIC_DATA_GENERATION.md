# Synthetic Credit Card Transaction Data Generation

## Overview

This document describes the improved synthetic data generation system for credit card fraud detection. The system generates realistic transaction patterns with both legitimate and fraudulent transactions, designed to train machine learning models for fraud detection.

## Key Improvements (December 2025)

The synthetic data generation has been significantly improved to create more realistic patterns:

### 1. Location-Aware Transaction Generation

**Problem Solved:** Previous implementation randomly sampled merchants from random countries for each transaction, causing consecutive transactions for the same cardholder to jump between continents, triggering many false positives for "impossible travel" fraud detection.

**Solution:** New `generate_credit_card_transactions_with_location_continuity()` function that:
- Assigns a home country to each cardholder
- Generates transactions in time-ordered sequences per card
- Maintains location continuity with realistic travel patterns:
  - **85%** of transactions in home country
  - **10%** in neighboring countries (local travel)
  - **5%** international travel
- Enforces minimum **6-hour gap** between location changes
- Uses IP addresses from the same subnet for consecutive transactions in the same country

### 2. Enhanced Fraud Patterns

#### Geographic Fraud
- Pairs of card-present transactions from distant countries
- **Varied time gaps** for impossible travel:
  - 5-15 minutes (33% - clearly impossible)
  - 15-30 minutes (33% - impossible for long distances)
  - 30-60 minutes (33% - still impossible for distant countries)
- Uses distant country pairs (e.g., US ↔ China, UK ↔ Australia)

#### Chain Attacks
- 5-15 transactions per attack over 10 minutes to 2 hours
- **Multiple IPs** (30% of attacks use 2-3 IPs from same country for sophistication)
- **Varied amounts** showing progression:
  - First 1-2 transactions: $1-10 (testing)
  - Next 2-3 transactions: $10-25 (building confidence)
  - Remaining: $25-50 (maximizing theft)
- **Burst timing patterns** (40% of transactions in bursts: 10s-2min apart)
- Always online (card_present=False)

## Architecture

### Data Generation Pipeline

```
1. Merchants → merchant_details feature group
2. Banks → bank_details feature group
3. Accounts → assign_cardholder_home_locations → account_details feature group
4. Cards → card_details feature group
5. Transactions (with location continuity) → fraud generation → feature groups
```

### Core Components

#### Geographic Data Structures

```python
# Country IP Ranges (synth_transactions.py:26-302)
COUNTRY_IP_RANGES = {
    "United States": [...],
    "United Kingdom": [...],
    # 25+ countries with real IP ranges
}

# Neighboring Countries (synth_transactions.py:310-339)
NEIGHBORING_COUNTRIES = {
    "United States": ["Canada", "Mexico"],
    "United Kingdom": ["France", "Germany", "Netherlands", ...],
    # Defines geographic neighbors for realistic travel
}
```

#### Key Functions

##### 1. `assign_cardholder_home_locations(account_df, seed=42)`
**Location:** `synth_transactions.py:573-615`

Assigns a home country to each account using weighted distribution:
- United States: 35%
- Major countries (UK, Germany, France, Canada, China, Japan): 8% each
- Other countries: 1-4%

```python
account_df = st.generate_account_details(...)
account_df = st.assign_cardholder_home_locations(account_df, seed=42)
```

##### 2. `generate_credit_card_transactions_with_location_continuity(...)`
**Location:** `synth_transactions.py:763-948`

Generates transactions with realistic geographic patterns.

**Parameters:**
- `card_df`: Card details
- `account_df`: Account details with 'home_country' column (required!)
- `merchant_df`: Merchant details
- `start_date`, `end_date`: Transaction date range
- `rows`: Number of transactions to generate
- `tid_offset`: Starting transaction ID
- `seed`: Random seed

**Returns:** DataFrame with columns: t_id, cc_num, account_id, merchant_id, amount, ip_address, card_present, ts

**Location Logic:**
- Generates time-ordered transactions per card
- Tracks current location throughout transaction sequence
- Only allows location changes after 6+ hours
- Chooses location type:
  - 85%: Stay in home country
  - 10%: Travel to neighboring country
  - 5%: International travel
- Uses same-subnet IPs (60% probability) for consecutive transactions in same country

##### 3. `generate_fraud(transaction_df, card_df, merchant_df, fraud_rate, chain_attack_ratio, seed)`
**Location:** `synth_transactions.py:1052-1330`

Generates fraudulent transactions with improved patterns.

**Parameters:**
- `transaction_df`: Legitimate transactions
- `card_df`: Card details
- `merchant_df`: Merchant details
- `fraud_rate`: Percentage of transactions that are fraudulent (e.g., 0.0001 = 0.01%)
- `chain_attack_ratio`: Ratio of chain attacks to total fraud (e.g., 0.9 = 90% chain, 10% geographic)
- `seed`: Random seed

**Returns:** `(updated_transaction_df, fraud_df)`
- `updated_transaction_df`: Original + fraudulent transactions
- `fraud_df`: Fraud metadata (t_id, cc_num, explanation, ts)

##### 4. `generate_ip_in_same_subnet(previous_ip, seed)`
**Location:** `synth_transactions.py:403-438`

Generates IP address in same subnet as previous IP.

**Subnet Selection:**
- 70%: Same /24 subnet (same first 3 octets)
- 30%: Same /16 subnet (same first 2 octets)

## Usage

### Basic Usage

```python
from ccfraud import synth_transactions as st
from datetime import datetime, timedelta

# 1. Generate base entities
current_date = datetime(2025, 12, 5)
merchants = st.generate_merchant_details(rows=500, ...)
banks = st.generate_bank_details(rows=1000, ...)

# 2. Generate accounts and ASSIGN HOME LOCATIONS
accounts = st.generate_account_details(rows=10000, ...)
accounts = st.assign_cardholder_home_locations(accounts, seed=42)  # CRITICAL!

# 3. Generate cards
cards = st.generate_card_details(rows=20000, ...)

# 4. Generate transactions with location continuity
transactions = st.generate_credit_card_transactions_with_location_continuity(
    card_df=cards,
    account_df=accounts,  # Must have 'home_country' column!
    merchant_df=merchants,
    start_date=current_date - timedelta(days=30),
    end_date=current_date,
    rows=500_000,
    seed=42
)

# 5. Add fraud
transactions, fraud_df = st.generate_fraud(
    transaction_df=transactions,
    card_df=cards,
    merchant_df=merchants,
    fraud_rate=0.0001,  # 0.01%
    chain_attack_ratio=0.9,  # 90% chain, 10% geographic
    seed=42
)
```

### Using the Data Generator Script

```bash
# Generate all entities (backfill mode)
python -m ccfraud.data_generator --mode backfill --num-transactions 500000

# Generate only new transactions (incremental mode)
python -m ccfraud.data_generator --mode incremental --entities transactions

# Custom parameters
python -m ccfraud.data_generator \
    --mode backfill \
    --num-transactions 1000000 \
    --fraud-rate 0.0002 \
    --chain-attack-ratio 0.85 \
    --seed 12345
```

## Testing

Comprehensive pytest tests are provided to validate the improvements.

### Running Tests

```bash
# Run all tests
cd /home/jdowling/Projects/mlfs-book/ccfraud
pytest

# Run specific test file
pytest tests/test_transaction_generation.py

# Run with verbose output
pytest -v

# Run specific test
pytest tests/test_fraud_generation.py::TestChainAttacks::test_chain_attacks_have_varied_amounts
```

### Test Coverage

#### Transaction Generation Tests (`test_transaction_generation.py`)
- ✅ Home location assignment
- ✅ Location continuity (85% home, 10% local, 5% international)
- ✅ Same-subnet IP usage for consecutive transactions
- ✅ Time-ordered transactions per card
- ✅ Neighboring countries mapping validity

#### Fraud Generation Tests (`test_fraud_generation.py`)
- ✅ Fraud rate accuracy
- ✅ Chain attack ratio
- ✅ Chain attack characteristics:
  - Multiple transactions per card
  - Small amounts (<$50)
  - Amount progression (small → larger)
  - Online only (card_present=False)
  - Multiple IPs from same country
- ✅ Geographic fraud characteristics:
  - Distant country pairs
  - Short time gaps (<1 hour)
  - Card-present transactions
  - Comes in pairs

## Expected Results

### Before Improvements
- Many legitimate transactions flagged as impossible travel
- Geographic fraud patterns too obvious (always 15-60 min gaps)
- Chain attacks too uniform (same amount, same IP)
- Poor model training due to high false positive rate

### After Improvements
- **Drastically fewer false positives** for geographic fraud
- **More varied fraud patterns** with realistic signatures
- **Better signal/noise separation** for ML training
- **Realistic cardholder behavior** with location continuity

## Data Schema

### Transactions DataFrame
| Column | Type | Description |
|--------|------|-------------|
| t_id | int | Transaction ID |
| cc_num | str | Credit card number (XXXX-XXXX-XXXX-XXXX) |
| account_id | str | Account ID (foreign key to accounts) |
| merchant_id | str | Merchant ID (foreign key to merchants) |
| amount | float | Transaction amount ($) |
| ip_address | str | IP address (XXX.XXX.XXX.XXX) |
| card_present | bool | Physical card used (True) or online (False) |
| ts | datetime | Transaction timestamp |

### Fraud DataFrame
| Column | Type | Description |
|--------|------|-------------|
| t_id | int | Transaction ID (foreign key to transactions) |
| cc_num | str | Credit card number |
| explanation | str | Fraud explanation (why flagged as fraud) |
| ts | datetime | Transaction timestamp |

### Accounts DataFrame (with home_country)
| Column | Type | Description |
|--------|------|-------------|
| account_id | str | Account ID |
| name | str | Account holder name |
| address | str | Account holder address |
| debt_end_prev_month | float | Debt at end of previous month |
| last_modified | datetime | Last modification timestamp |
| creation_date | datetime | Account creation date |
| end_date | datetime | Account closure date (null if active) |
| **home_country** | **str** | **Cardholder's home country (NEW)** |

## Migration Guide

### Migrating from Old Function

**Old code:**
```python
transaction_df = st.generate_credit_card_transactions_from_existing(
    card_df=card_df,
    merchant_df=merchant_df,
    start_date=start_date,
    end_date=end_date,
    rows=num_transactions,
    seed=42
)
```

**New code:**
```python
# STEP 1: Ensure accounts have home_country
if "home_country" not in account_df.columns:
    account_df = st.assign_cardholder_home_locations(account_df, seed=42)

# STEP 2: Use new function with account_df
transaction_df = st.generate_credit_card_transactions_with_location_continuity(
    card_df=card_df,
    account_df=account_df,  # NEW: Required with home_country column
    merchant_df=merchant_df,
    start_date=start_date,
    end_date=end_date,
    rows=num_transactions,
    seed=42
)
```

### Backward Compatibility

The old `generate_credit_card_transactions_from_existing()` function is still available but **deprecated**. It will generate warnings about potential false positives.

## Troubleshooting

### Error: "account_df must have 'home_country' column"

**Solution:** Call `assign_cardholder_home_locations()` before transaction generation:
```python
account_df = st.assign_cardholder_home_locations(account_df, seed=42)
```

### Too many/few fraud transactions generated

**Explanation:** Fraud count is approximate due to attack patterns:
- Chain attacks generate 5-15 transactions per attack
- Geographic fraud generates pairs (2 transactions)

The actual fraud count may vary ±50% from `fraud_rate * num_transactions`.

### Geographic fraud still has false positives

**Check:**
1. Are you using the NEW transaction generation function?
2. Did you assign home locations to accounts?
3. Are consecutive transactions for the same card in different distant countries?
   - If yes, check the time gap - should be >6 hours for legitimate travel

## Performance

### Generation Speed
- **Accounts** (10K): ~1 second
- **Cards** (20K): ~2 seconds
- **Transactions** (500K with location continuity): ~30-60 seconds
- **Fraud** (500 fraud records): ~5 seconds

### Memory Usage
- **500K transactions**: ~50-100 MB
- **1M transactions**: ~100-200 MB

## References

### Code Files
- Main module: `ccfraud/synth_transactions.py`
- Data generator: `ccfraud/data_generator.py`
- Tests: `tests/test_transaction_generation.py`, `tests/test_fraud_generation.py`

### Notebooks
- Data generation: `notebooks/0-data-generation-with-polars.ipynb`

### Related Documentation
- See `CLAUDE.md` for project overview
- See `README.md` for general project information

## Contributing

When modifying synthetic data generation:

1. **Update tests** in `tests/` directory
2. **Run pytest** to ensure all tests pass
3. **Update this documentation** if changing behavior
4. **Maintain backward compatibility** when possible
5. **Use seed parameters** for reproducibility

## Changelog

### Version 2.0 (December 2025)
- ✨ Added location-aware transaction generation
- ✨ Added home location assignment for cardholders
- ✨ Improved geographic fraud patterns (varied time gaps)
- ✨ Improved chain attack patterns (multiple IPs, varied amounts, burst timing)
- ✨ Added same-subnet IP generation
- ✨ Added neighboring countries mapping
- 📝 Added comprehensive tests (pytest)
- 📝 Added detailed documentation

### Version 1.0 (Pre-December 2025)
- Initial synthetic data generation
- Basic fraud patterns
- Random transaction location selection
