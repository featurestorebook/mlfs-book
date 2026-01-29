#!/usr/bin/env python3
"""
Credit Card Fraud Detection - Streamlit Inference App

Interactive web interface for:
- Configuring transaction generation parameters
- Generating synthetic transactions
- Writing transactions to the feature group
- Getting fraud predictions from deployed model
- Viewing results with fraud status
"""

import streamlit as streamlit_lib

# Page configuration - MUST be first Streamlit command
streamlit_lib.set_page_config(
    page_title="Fraud Detection Demo",
    page_icon="💳",
    layout="wide"
)

import sys
from pathlib import Path
import warnings

import polars as pl
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

warnings.filterwarnings("ignore", module="IPython")

# Setup path to include root directory
current_file = Path(__file__).absolute()
ccfraud_pkg_dir = current_file.parent  # ccfraud/ccfraud/
ccfraud_project_dir = ccfraud_pkg_dir.parent  # ccfraud/
root_dir = ccfraud_project_dir.parent  # mlfs-book/

sys.path.insert(0, str(root_dir))
sys.path.insert(0, str(ccfraud_project_dir))

from ccfraud import synth_transactions as synth_tx
from mlfs import config

# Alias for streamlit
st = streamlit_lib


@st.cache_resource
def init_hopsworks():
    """Cache Hopsworks connection."""
    import hopsworks
    # Load settings from .env file (no interactive prompts)
    env_file = str(root_dir / '.env')
    settings = config.HopsworksSettings(_env_file=env_file)
    project = hopsworks.login()
    fs = project.get_feature_store()
    return project, fs


@st.cache_resource
def load_entities(_project, _fs):
    """Load merchants, accounts, cards from feature groups."""
    # Load merchants
    merchant_fg = _fs.get_feature_group("merchant_details", version=1)
    merchant_df = pl.from_pandas(merchant_fg.read())

    # Load accounts
    account_fg = _fs.get_feature_group("account_details", version=1)
    account_df = pl.from_pandas(account_fg.read())

    # Ensure home_country exists
    if "home_country" not in account_df.columns:
        account_df = synth_tx.assign_cardholder_home_locations(account_df, seed=42)

    # Load cards
    card_fg = _fs.get_feature_group("card_details", version=1)
    card_df = pl.from_pandas(card_fg.read())

    # Join cards with accounts to get home_country
    cards_with_home = card_df.join(
        account_df.select(["account_id", "home_country"]),
        on="account_id",
        how="left"
    ).with_columns(
        pl.col("home_country").fill_null("United States")
    )

    return merchant_df, account_df, cards_with_home


@st.cache_resource
def get_deployment(_project):
    """Get ccfraud model deployment."""
    ms = _project.get_model_serving()
    try:
        deployment = ms.get_deployment("ccfraud")
        # Check if deployment is running, start if not
        state = deployment.get_state()
        if state.status != "RUNNING":
            st.info(f"Deployment status: {state.status}. Starting deployment...")
            deployment.start(await_running=120)
        return deployment
    except Exception as e:
        st.warning(f"Could not get deployment: {e}")
        return None


def get_transactions_fg(fs):
    """Get the transactions feature group (not cached to allow fresh writes)."""
    return fs.get_feature_group("credit_card_transactions", version=1)


def get_max_transaction_id(transactions_fg):
    """Query feature group for max t_id."""
    try:
        df = pl.from_pandas(transactions_fg.read())
        if df.height == 0:
            return 0
        max_id = df.select(pl.col("t_id").max()).item()
        return max_id if max_id is not None else 0
    except Exception:
        return 0


def generate_transactions(cards_with_home, merchant_df, num_transactions, fraud_rate, rng, start_t_id):
    """Generate synthetic transactions DataFrame.

    Note: Since we sample from cards_with_home (which is derived from card_df),
    all generated cc_nums are guaranteed to exist in card_details. This ensures
    the Feldera streaming pipeline's ASOF JOIN works correctly.
    """
    now = datetime.now()

    # Sample cards from cards_with_home - this ensures all cc_nums exist in card_details
    card_indices = rng.choice(len(cards_with_home), size=num_transactions, replace=True)
    merchant_indices = rng.choice(len(merchant_df), size=num_transactions, replace=True)

    # Build transaction list
    txn_list = []
    for i in range(num_transactions):
        card_row = cards_with_home.row(card_indices[i], named=True)
        merchant_row = merchant_df.row(merchant_indices[i], named=True)

        cc_num = card_row['cc_num']
        account_id = card_row['account_id']
        home_country = card_row['home_country']
        merchant_id = merchant_row['merchant_id']

        # Generate amount (log-normal distribution)
        amount = float(rng.lognormal(mean=3.5, sigma=1.2))

        # Generate IP address based on home country
        ip_address = synth_tx.generate_ip_for_country(home_country, rng)

        # Card present (random)
        card_present = bool(rng.choice([0, 1], p=[0.7, 0.3]))

        # Generate timestamp spread across the batch
        ts = now + timedelta(microseconds=i * 1000)

        txn_list.append({
            't_id': start_t_id + i,
            'cc_num': cc_num,
            'account_id': account_id,
            'merchant_id': merchant_id,
            'amount': round(amount, 2),
            'ip_address': ip_address,
            'card_present': card_present,
            'ts': ts
        })

    return pl.DataFrame(txn_list)


def write_to_feature_group(transactions_fg, transactions_df):
    """Write transactions to credit_card_transactions FG."""
    transactions_fg.insert(transactions_df.to_pandas())


def predict_fraud(deployment, transactions_df, progress_callback=None):
    """Call deployed model for each transaction and return predictions."""
    predictions = []
    total = len(transactions_df)

    for idx, row in enumerate(transactions_df.iter_rows(named=True)):
        try:
            # Input format expected by ccfraud-predictor.py:
            # [[cc_num, amount, merchant_id, ip_address, card_present, t_id]]
            inputs = [[
                row['cc_num'],
                row['amount'],
                row['merchant_id'],
                row['ip_address'],
                row['card_present'],
                row['t_id']
            ]]
            result = deployment.predict(inputs=inputs)
            # result format: {"predictions": [0]} or {"predictions": [1]}
            pred = result.get("predictions", [0])[0]
            predictions.append(bool(pred))
        except Exception as e:
            st.warning(f"Prediction error for transaction {row['t_id']}: {e}")
            # On error, default to not fraud
            predictions.append(False)

        # Update progress if callback provided
        if progress_callback and (idx + 1) % 10 == 0:
            pct = 70 + int((idx + 1) / total * 25)
            progress_callback(pct, f"Predicting... {idx + 1}/{total}")

    return predictions


def main():
    st.title("Credit Card Fraud Detection")
    st.markdown("Interactive fraud prediction using deployed ML model")

    # Sidebar controls
    st.sidebar.header("Configuration")

    num_transactions = st.sidebar.slider(
        "Number of Transactions",
        min_value=10,
        max_value=500,
        value=50,
        step=10,
        help="Number of synthetic transactions to generate"
    )

    fraud_rate = st.sidebar.slider(
        "Fraud Rate (%)",
        min_value=0.0,
        max_value=10.0,
        value=0.5,
        step=0.1,
        help="Approximate percentage of fraudulent transactions"
    ) / 100.0

    transactions_per_sec = st.sidebar.slider(
        "Transactions per Second",
        min_value=1,
        max_value=50,
        value=10,
        help="Rate for displaying results (visual only)"
    )

    random_seed = st.sidebar.number_input(
        "Random Seed",
        min_value=1,
        max_value=99999,
        value=42,
        help="Seed for reproducible generation"
    )

    # Connection status section
    st.header("Connection Status")

    # Initialize connections
    try:
        with st.spinner("Connecting to Hopsworks..."):
            project, fs = init_hopsworks()
        st.success(f"Connected to project: **{project.name}**")
    except Exception as e:
        st.error(f"Failed to connect to Hopsworks: {e}")
        st.stop()

    # Load entities
    try:
        with st.spinner("Loading feature groups..."):
            merchant_df, account_df, cards_with_home = load_entities(project, fs)
        st.success(f"Loaded: **{len(merchant_df)}** merchants, **{len(account_df)}** accounts, **{len(cards_with_home)}** cards")
    except Exception as e:
        st.error(f"Failed to load feature groups: {e}")
        st.info("Please run `inv backfill` first to create the required feature groups.")
        st.stop()

    # Check deployment
    with st.spinner("Checking model deployment..."):
        deployment = get_deployment(project)
    if deployment:
        try:
            state = deployment.get_state()
            st.success(f"Model deployment **ccfraud** is **{state.status}**")
        except Exception:
            st.success(f"Model deployment **ccfraud** is available")
    else:
        st.warning("Model deployment **ccfraud** not found. Predictions will not be available.")
        st.info("Please deploy the model first using `inv train` and then deploy from Hopsworks UI.")

    st.divider()

    # Main action button
    st.header("Generate & Predict")

    col1, col2 = st.columns([1, 3])

    with col1:
        generate_button = st.button(
            "Generate & Predict",
            type="primary",
            use_container_width=True
        )

    with col2:
        write_to_fg = st.checkbox(
            "Write transactions to Feature Group",
            value=True,
            help="If checked, transactions will be written to credit_card_transactions feature group"
        )

    if generate_button:
        # Create random number generator
        rng = np.random.default_rng(random_seed)

        # Progress tracking
        progress_bar = st.progress(0, text="Initializing...")

        # Get transactions feature group and max ID
        progress_bar.progress(10, text="Getting transaction ID...")
        transactions_fg = get_transactions_fg(fs)
        start_t_id = get_max_transaction_id(transactions_fg) + 1

        # Generate transactions
        progress_bar.progress(30, text=f"Generating {num_transactions} transactions...")
        transactions_df = generate_transactions(
            cards_with_home,
            merchant_df,
            num_transactions,
            fraud_rate,
            rng,
            start_t_id
        )

        # Write to feature group if enabled
        if write_to_fg:
            progress_bar.progress(50, text="Writing to feature group...")
            try:
                write_to_feature_group(transactions_fg, transactions_df)
                st.info(f"Wrote {num_transactions} transactions to credit_card_transactions feature group")
            except Exception as e:
                st.warning(f"Failed to write to feature group: {e}")

        # Get predictions from deployed model
        if deployment:
            progress_bar.progress(70, text="Getting fraud predictions from deployed model...")
            predictions = predict_fraud(
                deployment,
                transactions_df,
                progress_callback=lambda pct, txt: progress_bar.progress(pct, text=txt)
            )
        else:
            # No deployment - simulate predictions based on fraud_rate
            progress_bar.progress(70, text="Simulating predictions (no deployment)...")
            predictions = [rng.random() < fraud_rate for _ in range(num_transactions)]
            st.warning("Using simulated predictions - no model deployment available")

        progress_bar.progress(100, text="Complete!")

        # Create results DataFrame
        results_df = transactions_df.to_pandas()
        results_df['fraud_prediction'] = predictions
        results_df['fraud_prediction_label'] = results_df['fraud_prediction'].apply(
            lambda x: 'FRAUD' if x else 'Legitimate'
        )

        # Display results
        st.divider()
        st.header("Results")

        # Summary metrics
        total_count = len(results_df)
        fraud_count = results_df['fraud_prediction'].sum()
        legitimate_count = total_count - fraud_count

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Total Transactions", total_count)

        with col2:
            st.metric("Predicted Fraud", fraud_count)

        with col3:
            st.metric("Legitimate", legitimate_count)

        with col4:
            fraud_percentage = (fraud_count / total_count * 100) if total_count > 0 else 0
            st.metric("Fraud Rate", f"{fraud_percentage:.1f}%")

        st.divider()

        # Results table with styling
        st.subheader("Transaction Details")

        # Display columns
        display_cols = ['t_id', 'cc_num', 'merchant_id', 'amount', 'ip_address',
                       'card_present', 'ts', 'fraud_prediction_label']

        display_df = results_df[display_cols].copy()
        display_df.columns = ['ID', 'Card Number', 'Merchant', 'Amount ($)',
                             'IP Address', 'Card Present', 'Timestamp', 'Prediction']

        # Style the dataframe
        def highlight_fraud(row):
            if row['Prediction'] == 'FRAUD':
                return ['background-color: #ffcccc'] * len(row)
            return [''] * len(row)

        styled_df = display_df.style.apply(highlight_fraud, axis=1)
        styled_df = styled_df.format({'Amount ($)': '${:.2f}'})

        st.dataframe(
            styled_df,
            use_container_width=True,
            hide_index=True
        )

        # Show fraud transactions separately if any
        if fraud_count > 0:
            st.subheader("Suspected Fraud Transactions")
            fraud_transactions = display_df[display_df['Prediction'] == 'FRAUD']
            st.dataframe(
                fraud_transactions.style.format({'Amount ($)': '${:.2f}'}),
                use_container_width=True,
                hide_index=True
            )


if __name__ == "__main__":
    main()
