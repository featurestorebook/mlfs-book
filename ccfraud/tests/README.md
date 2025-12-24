# Synthetic Data Generation Tests

This directory contains pytest tests for the improved synthetic credit card transaction data generation.

## Running Tests

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run specific test file
pytest tests/test_transaction_generation.py
pytest tests/test_fraud_generation.py

# Run specific test class
pytest tests/test_fraud_generation.py::TestChainAttacks

# Run specific test
pytest tests/test_fraud_generation.py::TestChainAttacks::test_chain_attacks_have_varied_amounts

# Run with coverage (if pytest-cov installed)
pytest --cov=ccfraud --cov-report=html
```

## Test Files

### `test_transaction_generation.py`
Tests for location-aware transaction generation:
- **TestHomeLocationAssignment**: Home country assignment to cardholders
- **TestLocationContinuityTransactions**: Geographic continuity in transactions
- **TestIPGeneration**: IP address generation functions
- **test_neighboring_countries_mapping**: Validation of country neighbor mappings

### `test_fraud_generation.py`
Tests for improved fraud patterns:
- **TestFraudGeneration**: General fraud generation tests
- **TestChainAttacks**: Chain attack pattern validation
- **TestGeographicFraud**: Geographic fraud pattern validation

## Test Coverage

All tests validate the improvements made in December 2025:

✅ **Location Continuity**
- 85% of transactions in home country
- 10% in neighboring countries
- 5% international travel
- 6-hour minimum between location changes
- Same-subnet IPs for consecutive transactions

✅ **Chain Attack Patterns**
- Multiple transactions per card (5-15)
- Amount progression (small → larger)
- Multiple IPs from same country (30% of attacks)
- Burst timing patterns (40% of transactions)
- Online only (card_present=False)

✅ **Geographic Fraud Patterns**
- Distant country pairs
- Short time gaps (5-60 minutes)
- Card-present transactions
- Comes in pairs

## Requirements

```bash
pip install pytest polars numpy
```

Optional:
```bash
pip install pytest-cov  # For coverage reports
```

## Expected Test Results

All tests should pass with the new implementation. If tests fail, check:

1. **Missing home_country column**: Ensure `assign_cardholder_home_locations()` was called
2. **Import errors**: Ensure you're in the correct directory
3. **Seed variations**: Tests use fixed seeds for reproducibility, but some tolerance is built in

## Adding New Tests

When adding new tests:

1. Follow the existing pattern with fixtures for sample data
2. Use descriptive test names starting with `test_`
3. Group related tests in classes starting with `Test`
4. Add docstrings explaining what each test validates
5. Use assertions with helpful error messages
6. Update this README with new test descriptions

## Debugging Failed Tests

```bash
# Run with extra verbosity and show local variables
pytest -vvl

# Stop at first failure
pytest -x

# Show print statements
pytest -s

# Run specific failing test with full output
pytest tests/test_fraud_generation.py::TestChainAttacks::test_chain_attacks_have_varied_amounts -vv -s
```
