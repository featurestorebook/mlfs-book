#!/usr/bin/env python3
"""
Credit Card Fraud Detection - Synthetic Data Generator

This script generates synthetic credit card transaction data with both legitimate
and fraudulent transactions. It supports two modes:
- Backfill: Create all feature groups from scratch
- Incremental: Use existing feature groups and generate only specified entities

Usage:
    # Backfill mode - generate all entities
    python data_generator.py --mode backfill

    # Incremental mode - generate only transactions
    python data_generator.py --mode incremental --entities transactions

    # Custom parameters
    python data_generator.py --mode backfill --num-transactions 100000 --fraud-rate 0.001
"""

import sys
from pathlib import Path
import warnings
import argparse
from datetime import datetime, timedelta
from typing import List, Optional
import hopsworks

warnings.filterwarnings("ignore", module="IPython")

# Setup path to include root directory
# __file__ is in ccfraud/ccfraud/data_generator.py
# We need to go up to mlfs-book to access both ccfraud and mlfs modules
current_file = Path(__file__).absolute()
ccfraud_pkg_dir = current_file.parent  # ccfraud/ccfraud/
ccfraud_project_dir = ccfraud_pkg_dir.parent  # ccfraud/
root_dir = ccfraud_project_dir.parent  # mlfs-book/

# Add root to path to access mlfs module
sys.path.insert(0, str(root_dir))
# Add ccfraud project dir to path
sys.path.insert(0, str(ccfraud_project_dir))

from ccfraud import synth_transactions as st
from mlfs import config


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate synthetic credit card transaction data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate all entities (backfill mode)
  %(prog)s --mode backfill

  # Generate only new transactions using existing entities
  %(prog)s --mode incremental --entities transactions

  # Generate specific entities with custom parameters
  %(prog)s --mode incremental --entities transactions --num-transactions 10000
        """
    )

    # Mode selection
    parser.add_argument(
        '--mode',
        choices=['backfill', 'incremental'],
        default='backfill',
        help='Generation mode: backfill (create all) or incremental (use existing)'
    )

    # Entity selection
    parser.add_argument(
        '--entities',
        nargs='+',
        choices=['merchants', 'banks', 'accounts', 'cards', 'transactions', 'fraud', 'all'],
        default=['all'],
        help='Which entities to generate (default: all)'
    )

    # Data generation parameters
    parser.add_argument(
        '--num-merchants',
        type=int,
        default=500,
        help='Number of merchants to generate (default: 500)'
    )

    parser.add_argument(
        '--num-banks',
        type=int,
        default=1000,
        help='Number of banks to generate (default: 1000)'
    )

    parser.add_argument(
        '--num-accounts',
        type=int,
        default=10000,
        help='Number of accounts to generate (default: 10000)'
    )

    parser.add_argument(
        '--num-cards',
        type=int,
        default=20000,
        help='Number of credit cards to generate (default: 20000)'
    )

    parser.add_argument(
        '--num-transactions',
        type=int,
        default=500000,
        help='Number of transactions to generate (default: 5000000)'
    )

    parser.add_argument(
        '--fraud-rate',
        type=float,
        default=0.001,
        help='Fraud rate as decimal (default: 0.0001 = 0.01%%)'
    )

    parser.add_argument(
        '--chain-attack-ratio',
        type=float,
        default=0.9,
        help='Ratio of chain attacks vs geographic fraud (default: 0.9 = 90%%)'
    )

    # Date parameters
    parser.add_argument(
        '--current-date',
        type=str,
        default=None,
        help='Current date in YYYY-MM-DD format (default: 2025-12-25)'
    )

    parser.add_argument(
        '--transaction-days',
        type=int,
        default=30,
        help='Number of days of transaction history to generate (default: 30)'
    )

    parser.add_argument(
        '--tid-offset',
        type=int,
        default=0,
        help='Starting offset for transaction IDs (default: 0)'
    )

    parser.add_argument(
        '--seed',
        type=int,
        default=42,
        help='Random seed for reproducibility (default: 42)'
    )

    # Environment
    parser.add_argument(
        '--env-file',
        type=str,
        default=None,
        help='Path to .env file (default: <root>/.env)'
    )

    return parser.parse_args()


class DataGenerator:
    """Manages synthetic data generation for credit card fraud detection."""

    def __init__(self, args):
        """Initialize the data generator with parsed arguments."""
        self.args = args
        self.mode = args.mode

        # Determine which entities to generate
        if 'all' in args.entities:
            self.entities = ['merchants', 'banks', 'accounts', 'cards', 'transactions', 'fraud']
        else:
            self.entities = args.entities
            # Auto-include fraud if transactions are generated
            if 'transactions' in self.entities and 'fraud' not in self.entities:
                self.entities.append('fraud')

        # Setup dates
        if args.current_date:
            self.current_date = datetime.strptime(args.current_date, '%Y-%m-%d')
        else:
            self.current_date = datetime(2025, 10, 5)

        self.transactions_start_date = self.current_date - timedelta(days=args.transaction_days)
        self.issue_date = self.current_date - timedelta(days=365 * 3)
        self.expiry_date = self.current_date + timedelta(days=365 * 3)
        self.account_creation_start_date = self.current_date - timedelta(days=365 * 5)
        self.account_last_modified_start_date = self.current_date - timedelta(days=365)
        self.bank_last_modified_start_date = self.current_date - timedelta(days=365)
        self.merchant_last_modified_start_date = self.current_date - timedelta(days=365)

        # Data storage
        self.merchant_df = None
        self.bank_df = None
        self.account_df = None
        self.card_df = None
        self.transaction_df = None
        self.fraud_df = None

        # Feature groups
        self.merchant_fg = None
        self.bank_fg = None
        self.account_fg = None
        self.card_fg = None
        self.transactions_fg = None
        self.fraud_fg = None

        # Initialize Hopsworks connection
        env_file = args.env_file or str(root_dir / '.env')
        print(f"Loading environment from: {env_file}")
        self.settings = config.HopsworksSettings(_env_file=env_file)

        print("\nConnecting to Hopsworks...")
        self.project = hopsworks.login()
        self.fs = self.project.get_feature_store()
        print(f"Connected to project: {self.project.name}")

    def should_generate(self, entity: str) -> bool:
        """Check if an entity should be generated."""
        return entity in self.entities

    def get_or_create_merchants(self):
        """Generate or load merchant data."""
        if not self.should_generate('merchants') and self.mode == 'incremental':
            print("\nLoading existing merchant data...")
            try:
                self.merchant_fg = self.fs.get_feature_group("merchant_details", version=1)
                self.merchant_df = self.merchant_fg.read()
                print(f"  Loaded {len(self.merchant_df)} merchants")
                return
            except Exception as e:
                print(f"  Warning: Could not load merchants, will generate: {e}")

        print("\nGenerating merchant data...")
        self.merchant_df = st.generate_merchant_details(
            rows=self.args.num_merchants,
            start_date=self.merchant_last_modified_start_date,
            end_date=self.current_date
        )

        if self.mode == 'backfill' or self.should_generate('merchants'):
            print("  Creating feature group: merchant_details")
            self.merchant_fg = st.create_feature_group_with_descriptions(
                self.fs,
                self.merchant_df,
                "merchant_details",
                "Details about merchants that execute transactions",
                ["merchant_id"],
                "last_modified",
                online_enabled=True
            )

        print(f"  Generated {len(self.merchant_df)} merchants")

    def get_or_create_banks(self):
        """Generate or load bank data."""
        if not self.should_generate('banks') and self.mode == 'incremental':
            print("\nLoading existing bank data...")
            try:
                self.bank_fg = self.fs.get_feature_group("bank_details", version=1)
                self.bank_df = self.bank_fg.read()
                print(f"  Loaded {len(self.bank_df)} banks")
                return
            except Exception as e:
                print(f"  Warning: Could not load banks, will generate: {e}")

        print("\nGenerating bank data...")
        self.bank_df = st.generate_bank_details(
            rows=self.args.num_banks,
            start_date=self.bank_last_modified_start_date,
            end_date=self.current_date
        )

        if self.mode == 'backfill' or self.should_generate('banks'):
            print("  Creating feature group: bank_details")
            self.bank_fg = st.create_feature_group_with_descriptions(
                self.fs,
                self.bank_df,
                "bank_details",
                "Details about banks that issue credit cards",
                ["bank_id"],
                "last_modified",
                online_enabled=True
            )

        print(f"  Generated {len(self.bank_df)} banks")

    def get_or_create_accounts(self):
        """Generate or load account data."""
        if not self.should_generate('accounts') and self.mode == 'incremental':
            print("\nLoading existing account data...")
            try:
                self.account_fg = self.fs.get_feature_group("account_details", version=1)
                self.account_df = self.account_fg.read()
                print(f"  Loaded {len(self.account_df)} accounts")

                # Ensure home_country exists for transaction generation
                if "home_country" not in self.account_df.columns:
                    print("  Warning: home_country not found, assigning home locations...")
                    self.account_df = st.assign_cardholder_home_locations(
                        self.account_df,
                        seed=self.args.seed
                    )
                return
            except Exception as e:
                print(f"  Warning: Could not load accounts, will generate: {e}")

        print("\nGenerating account data...")
        self.account_df = st.generate_account_details(
            rows=self.args.num_accounts,
            account_creation_start_date=self.account_creation_start_date,
            current_date=self.current_date,
            account_last_modified_start_date=self.account_last_modified_start_date
        )

        # IMPORTANT: Assign home locations for realistic geographic patterns
        print("  Assigning home locations to cardholders...")
        self.account_df = st.assign_cardholder_home_locations(
            self.account_df,
            seed=self.args.seed
        )

        if self.mode == 'backfill' or self.should_generate('accounts'):
            print("  Creating feature group: account_details")
            self.account_fg = st.create_feature_group_with_descriptions(
                self.fs,
                self.account_df,
                "account_details",
                "Information about the account and card",
                ["account_id"],
                "last_modified",
                online_enabled=True
            )

        print(f"  Generated {len(self.account_df)} accounts")

    def get_or_create_cards(self):
        """Generate or load card data."""
        if not self.should_generate('cards') and self.mode == 'incremental':
            print("\nLoading existing card data...")
            try:
                self.card_fg = self.fs.get_feature_group("card_details", version=1)
                self.card_df = self.card_fg.read()
                print(f"  Loaded {len(self.card_df)} cards")
                return
            except Exception as e:
                print(f"  Warning: Could not load cards, will generate: {e}")

        print("\nGenerating card data...")
        self.card_df = st.generate_card_details(
            rows=self.args.num_cards,
            num_accounts=self.args.num_accounts,
            num_banks=self.args.num_banks,
            current_date=self.current_date,
            issue_date=self.issue_date,
            expiry_date=self.expiry_date
        )

        if self.mode == 'backfill' or self.should_generate('cards'):
            print("  Creating feature group: card_details")
            self.card_fg = st.create_feature_group_with_descriptions(
                self.fs,
                self.card_df,
                "card_details",
                "Information about the account and card",
                ["cc_num"],
                "last_modified",
                topic_name=f"{self.project.name}_card_details_onlinefs",
                online_enabled=True
            )

        print(f"  Generated {len(self.card_df)} cards")

    def generate_transactions(self):
        """Generate transaction data with realistic location continuity."""
        if not self.should_generate('transactions'):
            print("\nSkipping transaction generation")
            return

        # Ensure we have required data
        if self.card_df is None:
            self.get_or_create_cards()
        if self.merchant_df is None:
            self.get_or_create_merchants()
        if self.account_df is None:
            self.get_or_create_accounts()

        # Verify account_df has home_country
        if "home_country" not in self.account_df.columns:
            print("  Warning: account_df missing home_country, assigning now...")
            self.account_df = st.assign_cardholder_home_locations(
                self.account_df,
                seed=self.args.seed
            )

        print("\nGenerating transaction data with location continuity...")
        print("  This generates realistic patterns where cardholders stay in their")
        print("  home country most of the time with appropriate travel patterns.")
        self.transaction_df = st.generate_credit_card_transactions_with_location_continuity(
            card_df=self.card_df,
            account_df=self.account_df,  # Must have 'home_country' column
            merchant_df=self.merchant_df,
            start_date=self.transactions_start_date,
            end_date=self.current_date,
            rows=self.args.num_transactions,
            tid_offset=self.args.tid_offset,
            seed=self.args.seed
        )

        print(f"  Generated {len(self.transaction_df)} transactions")

    def generate_fraud(self):
        """Generate fraud data based on transactions."""
        if not self.should_generate('fraud'):
            print("\nSkipping fraud generation")
            return

        # Ensure we have transaction data
        if self.transaction_df is None:
            print("\nWarning: Cannot generate fraud without transactions")
            return

        # Ensure we have card and merchant data for fraud generation
        if self.card_df is None:
            self.get_or_create_cards()
        if self.merchant_df is None:
            self.get_or_create_merchants()

        print("\nGenerating fraud data...")
        self.transaction_df, self.fraud_df = st.generate_fraud(
            transaction_df=self.transaction_df,
            card_df=self.card_df,
            merchant_df=self.merchant_df,
            fraud_rate=self.args.fraud_rate,
            chain_attack_ratio=self.args.chain_attack_ratio,
            seed=self.args.seed
        )

        print(f"  Generated {len(self.fraud_df)} fraudulent transactions")

    def save_transactions(self):
        """Save transaction and fraud data to feature groups."""
        if self.transaction_df is None:
            return

        print("\nSaving transaction data...")
        print("  Creating feature group: credit_card_transactions")
        self.transactions_fg = st.create_feature_group_with_descriptions(
            self.fs,
            self.transaction_df,
            "credit_card_transactions",
            "Details about credit card transactions",
            ["t_id"],
            "ts",
            topic_name=f"{self.project.name}_credit_card_transactions_onlinefs",
            online_enabled=True
        )

        if self.fraud_df is not None and len(self.fraud_df) > 0:
            print("  Creating feature group: cc_fraud")
            self.fraud_fg = st.create_feature_group_with_descriptions(
                self.fs,
                self.fraud_df,
                "cc_fraud",
                "Credit card transaction fraud",
                ["t_id"],
                "ts",
                online_enabled=False
            )

    def print_summary(self):
        """Print summary statistics."""
        print("\n" + "=" * 60)
        print("Data Generation Summary")
        print("=" * 60)
        print(f"Mode: {self.mode}")
        print(f"Entities: {', '.join(self.entities)}")
        print("\nGenerated Data:")

        if self.merchant_df is not None:
            print(f"  Merchants: {len(self.merchant_df):,} rows")
        if self.bank_df is not None:
            print(f"  Banks: {len(self.bank_df):,} rows")
        if self.account_df is not None:
            print(f"  Accounts: {len(self.account_df):,} rows")
        if self.card_df is not None:
            print(f"  Cards: {len(self.card_df):,} rows")
        if self.transaction_df is not None:
            print(f"  Transactions: {len(self.transaction_df):,} rows")
        if self.fraud_df is not None:
            print(f"  Fraudulent Transactions: {len(self.fraud_df):,} rows")
            fraud_rate_actual = len(self.fraud_df) / len(self.transaction_df) * 100
            print(f"  Actual Fraud Rate: {fraud_rate_actual:.4f}%")

        print("=" * 60)

    def run(self):
        """Execute the data generation process."""
        print("\n" + "=" * 60)
        print("Credit Card Fraud Detection - Data Generator")
        print("=" * 60)
        print(f"Mode: {self.mode}")
        print(f"Entities to generate: {', '.join(self.entities)}")
        print(f"Date range: {self.transactions_start_date.date()} to {self.current_date.date()}")
        print("=" * 60)

        # Generate entities in dependency order
        # Merchants and banks are independent
        if self.mode == 'backfill' or 'merchants' in self.entities:
            self.get_or_create_merchants()
        else:
            # Load existing if needed for transactions
            if 'transactions' in self.entities:
                self.get_or_create_merchants()

        if self.mode == 'backfill' or 'banks' in self.entities:
            self.get_or_create_banks()
        else:
            # Load existing if needed for cards
            if 'cards' in self.entities or 'transactions' in self.entities:
                self.get_or_create_banks()

        # Accounts are independent but needed for cards
        if self.mode == 'backfill' or 'accounts' in self.entities:
            self.get_or_create_accounts()
        else:
            # Load existing if needed for cards
            if 'cards' in self.entities or 'transactions' in self.entities:
                self.get_or_create_accounts()

        # Cards depend on banks and accounts
        if self.mode == 'backfill' or 'cards' in self.entities:
            self.get_or_create_cards()
        else:
            # Load existing if needed for transactions
            if 'transactions' in self.entities:
                self.get_or_create_cards()

        # Transactions depend on cards and merchants
        if 'transactions' in self.entities:
            self.generate_transactions()
            self.generate_fraud()
            self.save_transactions()

        # Print summary
        self.print_summary()

        print("\n✓ Data generation completed successfully!")


def main():
    """Main entry point."""
    args = parse_args()
    generator = DataGenerator(args)
    generator.run()


if __name__ == '__main__':
    main()
