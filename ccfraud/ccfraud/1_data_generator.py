#!/usr/bin/env python3
"""
Credit Card Fraud Detection - Synthetic Data Generator

This script generates synthetic credit card transaction data with both legitimate
and fraudulent transactions. It supports two modes:
- Backfill: Create all feature groups from scratch
- Incremental: Use existing feature groups and generate only specified entities

Usage:
    # Backfill mode - generate all entities for previous 30 days
    python data_generator.py --mode backfill

    # Incremental mode - generate only transactions
    python data_generator.py --mode incremental --entities transactions

    # Custom date range
    python data_generator.py --mode backfill --start-date 2025-11-01 --end-date 2025-12-01

    # Custom parameters
    python data_generator.py --mode backfill --num-transactions 100000 --fraud-rate 0.001
"""

import sys
from pathlib import Path
import warnings
import argparse
from datetime import datetime, timedelta
from typing import List, Optional
import time
import signal
import hopsworks
import polars as pl
import numpy as np

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
  # Generate all entities for previous 30 days (backfill mode)
  %(prog)s --mode backfill

  # Generate all entities for a specific date range
  %(prog)s --mode backfill --start-date 2025-11-01 --end-date 2025-12-01

  # Generate only new transactions using existing entities (previous 24 hours)
  %(prog)s --mode incremental --entities transactions --start-date 2025-12-24 --end-date 2025-12-25

  # Generate transactions with custom count
  %(prog)s --mode incremental --entities transactions --num-transactions 10000
        """
    )

    # Mode selection
    parser.add_argument(
        '--mode',
        choices=['backfill', 'incremental', 'streaming'],
        default='backfill',
        help='Generation mode: backfill (create all), incremental (use existing), streaming (continuous)'
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
        default=50,
        help='Number of merchants to generate (default: 500)'
    )

    parser.add_argument(
        '--num-banks',
        type=int,
        default=50,
        help='Number of banks to generate (default: 1000)'
    )

    parser.add_argument(
        '--num-accounts',
        type=int,
        default=1000,
        help='Number of accounts to generate (default: 10000)'
    )

    parser.add_argument(
        '--num-cards',
        type=int,
        default=2000,
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
        default=0.005,
        help='Fraud rate as decimal (default: 0.005 = 0.5%%)'
    )

    parser.add_argument(
        '--chain-attack-ratio',
        type=float,
        default=0.9,
        help='Ratio of chain attacks vs geographic fraud (default: 0.9 = 90%%)'
    )

    parser.add_argument(
        '--transactions-per-sec',
        type=int,
        default=10,
        help='Transactions per second for streaming mode (range: 1-100, default: 10)'
    )

    # Date parameters
    parser.add_argument(
        '--start-date',
        type=str,
        default=None,
        help='Start date for transactions in YYYY-MM-DD format (default: 30 days ago from end-date)'
    )

    parser.add_argument(
        '--end-date',
        type=str,
        default=None,
        help='End date for transactions in YYYY-MM-DD format (default: today)'
    )

    # Legacy parameters for backward compatibility
    parser.add_argument(
        '--current-date',
        type=str,
        default=None,
        help='(Legacy) Current date in YYYY-MM-DD format - use --end-date instead'
    )

    parser.add_argument(
        '--transaction-days',
        type=int,
        default=None,
        help='(Legacy) Number of days of transaction history - use --start-date instead'
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

        # Setup dates - prioritize new parameters, fall back to legacy, then defaults
        # End date: --end-date > --current-date > today
        if args.end_date:
            self.current_date = datetime.strptime(args.end_date, '%Y-%m-%d')
        elif args.current_date:
            self.current_date = datetime.strptime(args.current_date, '%Y-%m-%d')
            print(f"  Warning: --current-date is deprecated, use --end-date instead")
        else:
            self.current_date = datetime.now().replace(hour=23, minute=59, second=59)

        # Start date: --start-date > calculated from --transaction-days > 30 days ago
        if args.start_date:
            self.transactions_start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
        elif args.transaction_days is not None:
            self.transactions_start_date = self.current_date - timedelta(days=args.transaction_days)
            print(f"  Warning: --transaction-days is deprecated, use --start-date instead")
        else:
            self.transactions_start_date = self.current_date - timedelta(days=30)
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

        # Track what was actually inserted (not just loaded)
        self.inserted_counts = {}

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
                self.merchant_df = pl.from_pandas(self.merchant_fg.read())
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
            self.merchant_fg = st.get_or_create_feature_group_with_descriptions(
                self.fs,
                self.merchant_df,
                "merchant_details",
                "Details about merchants that execute transactions",
                ["merchant_id"],
                "last_modified",
                online_enabled=True
            )
            self.inserted_counts['merchants'] = len(self.merchant_df)
            print(f"  Inserted {len(self.merchant_df)} merchants")
        else:
            print(f"  Loaded {len(self.merchant_df)} merchants (not inserted)")

    def get_or_create_banks(self):
        """Generate or load bank data."""
        if not self.should_generate('banks') and self.mode == 'incremental':
            print("\nLoading existing bank data...")
            try:
                self.bank_fg = self.fs.get_feature_group("bank_details", version=1)
                self.bank_df = pl.from_pandas(self.bank_fg.read())
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
            self.bank_fg = st.get_or_create_feature_group_with_descriptions(
                self.fs,
                self.bank_df,
                "bank_details",
                "Details about banks that issue credit cards",
                ["bank_id"],
                "last_modified",
                online_enabled=True
            )
            self.inserted_counts['banks'] = len(self.bank_df)
            print(f"  Inserted {len(self.bank_df)} banks")
        else:
            print(f"  Loaded {len(self.bank_df)} banks (not inserted)")

    def get_or_create_accounts(self):
        """Generate or load account data."""
        if not self.should_generate('accounts') and self.mode == 'incremental':
            print("\nLoading existing account data...")
            try:
                self.account_fg = self.fs.get_feature_group("account_details", version=1)
                self.account_df = pl.from_pandas(self.account_fg.read())
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
            self.account_fg = st.get_or_create_feature_group_with_descriptions(
                self.fs,
                self.account_df,
                "account_details",
                "Information about the account and card",
                ["account_id"],
                "last_modified",
                online_enabled=True
            )
            self.inserted_counts['accounts'] = len(self.account_df)
            print(f"  Inserted {len(self.account_df)} accounts")
        else:
            print(f"  Loaded {len(self.account_df)} accounts (not inserted)")

    def get_or_create_cards(self):
        """Generate or load card data."""
        if not self.should_generate('cards') and self.mode == 'incremental':
            print("\nLoading existing card data...")
            try:
                self.card_fg = self.fs.get_feature_group("card_details", version=1)
                self.card_df = pl.from_pandas(self.card_fg.read())
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
            expiry_date=self.expiry_date,
            transactions_start_date=self.transactions_start_date
        )

        if self.mode == 'backfill' or self.should_generate('cards'):
            print("  Creating feature group: card_details")
            self.card_fg = st.get_or_create_feature_group_with_descriptions(
                self.fs,
                self.card_df,
                "card_details",
                "Information about the account and card",
                ["cc_num"],
                "last_modified",
                topic_name=f"{self.project.name}_card_details_onlinefs",
                online_enabled=True
            )
            self.inserted_counts['cards'] = len(self.card_df)
            print(f"  Inserted {len(self.card_df)} cards")
        else:
            print(f"  Loaded {len(self.card_df)} cards (not inserted)")

    def _get_max_transaction_id(self) -> int:
        """Query feature group for max t_id to continue numbering."""
        try:
            fg = self.fs.get_feature_group("credit_card_transactions", version=1)
            df = pl.from_pandas(fg.read())
            if df.height == 0:
                return 0
            max_id = df.select(pl.col("t_id").max()).item()
            return max_id if max_id is not None else 0
        except Exception as e:
            print(f"  Warning: Could not query max t_id, starting from 0: {e}")
            return 0

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

        # In incremental mode, ensure new unique transactions by:
        # 1. Continuing t_id numbering from where existing data left off
        # 2. Using a different seed so data values are different
        tid_offset = self.args.tid_offset
        self._effective_seed = self.args.seed
        if self.mode == 'incremental':
            max_existing_tid = self._get_max_transaction_id()
            if tid_offset <= max_existing_tid:
                tid_offset = max_existing_tid + 1
                print(f"  Incremental mode: continuing t_id from {tid_offset}")
            # Derive a new seed from the offset to ensure different data
            self._effective_seed = self.args.seed + tid_offset
            print(f"  Incremental mode: using seed {self._effective_seed} for unique data")

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
            tid_offset=tid_offset,
            seed=self._effective_seed
        )

        # Validate that all cc_nums exist in card_details (defensive check)
        self.transaction_df = st.validate_cc_nums_exist(self.transaction_df, self.card_df)

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
        # Use the effective seed (adjusted in incremental mode to avoid duplicates)
        fraud_seed = getattr(self, '_effective_seed', self.args.seed)
        self.transaction_df, self.fraud_df = st.generate_fraud(
            transaction_df=self.transaction_df,
            card_df=self.card_df,
            merchant_df=self.merchant_df,
            fraud_rate=self.args.fraud_rate,
            chain_attack_ratio=self.args.chain_attack_ratio,
            seed=fraud_seed
        )

        print(f"  Generated {len(self.fraud_df)} fraudulent transactions")

    def save_transactions(self):
        """Save transaction and fraud data to feature groups."""
        if self.transaction_df is None:
            return

        print("\nSaving transaction data...")
        print("  Creating feature group: credit_card_transactions")
        self.transactions_fg = st.get_or_create_feature_group_with_descriptions(
            self.fs,
            self.transaction_df,
            "credit_card_transactions",
            "Details about credit card transactions",
            ["t_id"],
            "ts",
            topic_name=f"{self.project.name}_credit_card_transactions_onlinefs",
            online_enabled=True,
            ttl_enabled=False
        )
        self.inserted_counts['transactions'] = len(self.transaction_df)
        print(f"  Inserted {len(self.transaction_df)} transactions")

        if self.fraud_df is not None and len(self.fraud_df) > 0:
            print("  Creating feature group: cc_fraud")
            self.fraud_fg = st.get_or_create_feature_group_with_descriptions(
                self.fs,
                self.fraud_df,
                "cc_fraud",
                "Credit card transaction fraud",
                ["t_id"],
                "ts",
                online_enabled=False
            )
            self.inserted_counts['fraud'] = len(self.fraud_df)
            print(f"  Inserted {len(self.fraud_df)} fraud records")
        else:
            print("  No fraud data to insert")

    def print_summary(self):
        """Print summary statistics."""
        print("\n" + "=" * 60)
        print("Data Generation Summary")
        print("=" * 60)
        print(f"Mode: {self.mode}")
        print(f"Entities requested: {', '.join(self.entities)}")

        if self.inserted_counts:
            print("\nInserted Data:")
            if 'merchants' in self.inserted_counts:
                print(f"  Merchants: {self.inserted_counts['merchants']:,} rows")
            if 'banks' in self.inserted_counts:
                print(f"  Banks: {self.inserted_counts['banks']:,} rows")
            if 'accounts' in self.inserted_counts:
                print(f"  Accounts: {self.inserted_counts['accounts']:,} rows")
            if 'cards' in self.inserted_counts:
                print(f"  Cards: {self.inserted_counts['cards']:,} rows")
            if 'transactions' in self.inserted_counts:
                print(f"  Transactions: {self.inserted_counts['transactions']:,} rows")
            if 'fraud' in self.inserted_counts:
                print(f"  Fraudulent Transactions: {self.inserted_counts['fraud']:,} rows")
                fraud_rate_actual = self.inserted_counts['fraud'] / self.inserted_counts['transactions'] * 100
                print(f"  Actual Fraud Rate: {fraud_rate_actual:.4f}%")
        else:
            print("\nNo data was inserted (all entities were loaded from existing feature groups)")

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


class StreamingGenerator:
    """Manages continuous streaming transaction generation."""

    def __init__(self, args):
        """Initialize the streaming generator with parsed arguments."""
        self.args = args
        self.tps = args.transactions_per_sec

        # Validate TPS range
        if not (1 <= self.tps <= 100):
            print(f"ERROR: --transactions-per-sec must be between 1-100 (got {self.tps})")
            sys.exit(1)

        # Initialize Hopsworks connection
        env_file = args.env_file or str(root_dir / '.env')
        print(f"Loading environment from: {env_file}")
        self.settings = config.HopsworksSettings(_env_file=env_file)

        print("\nConnecting to Hopsworks...")
        self.project = hopsworks.login()
        self.fs = self.project.get_feature_store()
        print(f"Connected to project: {self.project.name}")

        # State tracking
        self.shutdown_requested = False
        self.current_t_id = 0
        self.last_ip_per_card = {}  # Track IP continuity: {cc_num: (ip, timestamp, country)}
        self.total_transactions = 0
        self.total_fraud = 0

        # Random number generator
        self.rng = np.random.default_rng(args.seed)

        # Setup signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)

        # Load entities and setup feature groups
        print("\nLoading existing entities...")
        self._load_entities()
        self._get_feature_groups()

        # Get max transaction ID to continue numbering
        self.current_t_id = self._get_max_transaction_id() + 1
        print(f"  Starting from transaction ID: {self.current_t_id}")

    def _signal_handler(self, sig, frame):
        """Handle Ctrl-C gracefully."""
        print("\n\n⚠️  Shutdown requested... completing current batch...")
        self.shutdown_requested = True

    def _load_entities(self):
        """Load existing merchants, banks, accounts, and cards from feature groups."""
        try:
            # Load merchants
            merchant_fg = self.fs.get_feature_group("merchant_details", version=1)
            self.merchant_df = pl.from_pandas(merchant_fg.read())
            print(f"  Loaded {len(self.merchant_df)} merchants")

            # Load banks
            bank_fg = self.fs.get_feature_group("bank_details", version=1)
            self.bank_df = pl.from_pandas(bank_fg.read())
            print(f"  Loaded {len(self.bank_df)} banks")

            # Load accounts with home_country
            account_fg = self.fs.get_feature_group("account_details", version=1)
            self.account_df = pl.from_pandas(account_fg.read())

            # Ensure home_country exists
            if "home_country" not in self.account_df.columns:
                print("  Assigning home locations to accounts...")
                self.account_df = st.assign_cardholder_home_locations(
                    self.account_df,
                    seed=self.args.seed
                )
            print(f"  Loaded {len(self.account_df)} accounts")

            # Load cards
            card_fg = self.fs.get_feature_group("card_details", version=1)
            self.card_df = pl.from_pandas(card_fg.read())
            print(f"  Loaded {len(self.card_df)} cards")

            # Join cards with accounts to get home_country
            self.cards_with_home = self.card_df.join(
                self.account_df.select(["account_id", "home_country"]),
                on="account_id",
                how="left"
            ).with_columns(
                pl.col("home_country").fill_null("United States")
            )

        except Exception as e:
            print(f"\nERROR: Could not load existing entities: {e}")
            print("Please run backfill mode first:")
            print("  inv backfill-1")
            sys.exit(1)

    def _get_feature_groups(self):
        """Get references to feature groups for insertion."""
        try:
            self.transactions_fg = self.fs.get_feature_group("credit_card_transactions", version=1)
            self.fraud_fg = self.fs.get_feature_group("cc_fraud", version=1)
            print(f"  Feature groups ready for streaming")
        except Exception as e:
            print(f"\nERROR: Could not get feature groups: {e}")
            print("Please run backfill mode first:")
            print("  inv backfill-1")
            sys.exit(1)

    def _get_max_transaction_id(self) -> int:
        """Query feature group for max t_id to continue numbering."""
        try:
            df = pl.from_pandas(self.transactions_fg.read())
            if df.height == 0:
                return 0
            max_id = df.select(pl.col("t_id").max()).item()
            return max_id if max_id is not None else 0
        except Exception as e:
            print(f"  Warning: Could not query max t_id, starting from 0: {e}")
            return 0

    def _calculate_batch_params(self):
        """Calculate optimal batch size and sleep interval."""
        if self.tps >= 10:
            batch_size = 10
            interval = 10.0 / self.tps  # seconds between batches
        else:
            batch_size = max(1, self.tps)
            interval = 1.0
        return batch_size, interval

    def _generate_batch(self, batch_size: int):
        """Generate a micro-batch of transactions with real-time timestamps."""
        # Create timestamps for this batch (spread across microseconds for ordering)
        now = datetime.now()
        timestamps = [now + timedelta(microseconds=i * 1000) for i in range(batch_size)]

        # Sample cards from cards_with_home (which is joined from card_df)
        # This ensures all cc_nums exist in card_details for proper Feldera ASOF JOIN
        card_indices = self.rng.choice(len(self.cards_with_home), size=batch_size, replace=True)
        merchant_indices = self.rng.choice(len(self.merchant_df), size=batch_size, replace=True)

        # Build transaction dataframe
        txn_list = []
        for i in range(batch_size):
            card_row = self.cards_with_home.row(card_indices[i], named=True)
            merchant_row = self.merchant_df.row(merchant_indices[i], named=True)

            cc_num = card_row['cc_num']
            account_id = card_row['account_id']
            home_country = card_row['home_country']
            merchant_id = merchant_row['merchant_id']

            # Generate amount (log-normal distribution)
            amount = float(self.rng.lognormal(mean=3.5, sigma=1.2))

            # Generate IP address with continuity
            ip_address, country = self._generate_ip_with_continuity(cc_num, home_country)

            # Card present (random)
            card_present = bool(self.rng.choice([0, 1], p=[0.7, 0.3]))

            txn_list.append({
                't_id': self.current_t_id + i,
                'cc_num': cc_num,
                'account_id': account_id,
                'merchant_id': merchant_id,
                'amount': amount,
                'ip_address': ip_address,
                'card_present': card_present,
                'ts': timestamps[i]
            })

            # Update IP state
            self.last_ip_per_card[cc_num] = (ip_address, timestamps[i], country)

        # Create DataFrame
        txn_df = pl.DataFrame(txn_list)

        # Increment transaction ID counter
        self.current_t_id += batch_size

        return txn_df

    def _generate_ip_with_continuity(self, cc_num: str, home_country: str):
        """Generate IP address with location continuity."""
        # Check if card has recent IP state
        if cc_num in self.last_ip_per_card:
            last_ip, last_ts, last_country = self.last_ip_per_card[cc_num]

            # 60% chance to stay in same location (same subnet)
            if self.rng.random() < 0.6:
                # Keep same IP or generate from same subnet
                return last_ip, last_country

        # Generate new IP based on location distribution
        location_choice = self.rng.choice(
            ["home", "local", "international"],
            p=[0.85, 0.10, 0.05]
        )

        if location_choice == "home":
            country = home_country
        elif location_choice == "local":
            # Neighboring country (simplified - just pick a random country)
            country = self.rng.choice(list(st.COUNTRY_IP_RANGES.keys()))
        else:
            # International travel
            country = self.rng.choice(list(st.COUNTRY_IP_RANGES.keys()))

        # Generate IP for this country
        ip_address = st.generate_ip_for_country(country, self.rng)

        return ip_address, country

    def _generate_fraud_for_batch(self, txn_df: pl.DataFrame):
        """Generate fraud records for a batch of transactions."""
        if len(txn_df) == 0:
            return pl.DataFrame(schema={"t_id": pl.Int64, "cc_num": pl.Utf8, "explanation": pl.Utf8, "ts": pl.Datetime})

        # Use existing fraud generation logic
        try:
            _, fraud_df = st.generate_fraud(
                transaction_df=txn_df,
                card_df=self.card_df,
                merchant_df=self.merchant_df,
                fraud_rate=self.args.fraud_rate,
                chain_attack_ratio=self.args.chain_attack_ratio,
                seed=self.args.seed + self.total_transactions  # Vary seed
            )
            return fraud_df
        except Exception as e:
            print(f"  Warning: Could not generate fraud for batch: {e}")
            return pl.DataFrame(schema={"t_id": pl.Int64, "cc_num": pl.Utf8, "explanation": pl.Utf8, "ts": pl.Datetime})

    def _insert_batch(self, txn_df: pl.DataFrame, fraud_df: pl.DataFrame):
        """Insert batch to feature groups (publishes to Kafka automatically)."""
        try:
            # Insert transactions (automatically publishes to Kafka)
            self.transactions_fg.multi_part_insert(txn_df.to_pandas())

            # Insert fraud if any
            if fraud_df is not None and len(fraud_df) > 0:
                self.fraud_fg.multi_part_insert(fraud_df.to_pandas())

        except Exception as e:
            print(f"\n  WARNING: Failed to insert batch: {e}")
            print("  Retrying in 5 seconds...")
            time.sleep(5)
            # Retry once
            try:
                self.transactions_fg.multi_part_insert(txn_df.to_pandas())
                if fraud_df is not None and len(fraud_df) > 0:
                    self.fraud_fg.multi_part_insert(fraud_df.to_pandas())
            except Exception as e2:
                print(f"  ERROR: Retry failed: {e2}")
                print("  Skipping this batch...")

    def _cleanup_ip_state(self):
        """Limit IP state tracking to prevent memory growth."""
        MAX_TRACKED_CARDS = 10000
        if len(self.last_ip_per_card) > MAX_TRACKED_CARDS:
            # Keep only the most recent 50%
            sorted_by_time = sorted(
                self.last_ip_per_card.items(),
                key=lambda x: x[1][1],  # Sort by timestamp
                reverse=True
            )
            self.last_ip_per_card = dict(sorted_by_time[:MAX_TRACKED_CARDS // 2])

    def print_summary(self):
        """Print final summary statistics."""
        print("\n" + "=" * 60)
        print("Streaming Transaction Generator - Summary")
        print("=" * 60)
        print(f"Total Transactions Generated: {self.total_transactions:,}")
        print(f"Total Fraud Records: {self.total_fraud:,}")
        if self.total_transactions > 0:
            fraud_rate_actual = self.total_fraud / self.total_transactions * 100
            print(f"Actual Fraud Rate: {fraud_rate_actual:.4f}%")
        print(f"Final Transaction ID: {self.current_t_id - 1}")
        print("=" * 60)

    def run(self):
        """Main streaming loop with drift-correcting rate limiter."""
        batch_size, target_interval = self._calculate_batch_params()

        print("\n" + "=" * 60)
        print("Credit Card Fraud Detection - Streaming Generator")
        print("=" * 60)
        print(f"Rate: {self.tps} transactions/second")
        print(f"Batch size: {batch_size} transactions")
        print(f"Batch interval: {target_interval:.3f} seconds")
        print(f"Fraud rate: {self.args.fraud_rate * 100:.2f}%")
        print(f"Publishing to: credit_card_transactions FG → Kafka")
        print("\nPress Ctrl-C to stop gracefully...")
        print("=" * 60 + "\n")

        start_time = time.time()
        batch_count = 0
        last_report_time = start_time

        try:
            while not self.shutdown_requested:
                batch_start = time.time()

                # Generate batch
                txn_df = self._generate_batch(batch_size)

                # Generate fraud for batch
                fraud_df = self._generate_fraud_for_batch(txn_df)

                # Insert batch (publishes to Kafka automatically)
                self._insert_batch(txn_df, fraud_df)

                # Update counters
                self.total_transactions += len(txn_df)
                self.total_fraud += len(fraud_df) if fraud_df is not None else 0
                batch_count += 1

                # Cleanup IP state periodically
                if batch_count % 100 == 0:
                    self._cleanup_ip_state()

                # Progress reporting
                elapsed = time.time() - start_time
                if batch_count % 10 == 0 or (elapsed - (last_report_time - start_time)) > 30:
                    actual_tps = self.total_transactions / elapsed if elapsed > 0 else 0
                    print(f"  [{elapsed:.0f}s] Generated {self.total_transactions:,} txns "
                          f"| Rate: {actual_tps:.1f} TPS "
                          f"| Target: {self.tps} TPS "
                          f"| Fraud: {self.total_fraud:,}")
                    last_report_time = time.time()

                # Drift-correcting sleep
                expected_time = start_time + (batch_count * target_interval)
                sleep_time = max(0, expected_time - time.time())
                if sleep_time > 0:
                    time.sleep(sleep_time)

        except Exception as e:
            print(f"\n\nERROR: Streaming failed: {e}")
            import traceback
            traceback.print_exc()

        finally:
            self.transactions_fg.finalize_multi_part_insert()
            self.fraud_fg.finalize_multi_part_insert()            
            # Print summary on exit
            self.print_summary()
            print("\n✓ Streaming stopped gracefully!")


def main():
    """Main entry point."""
    args = parse_args()

    if args.mode == 'streaming':
        generator = StreamingGenerator(args)
    else:
        generator = DataGenerator(args)

    generator.run()


if __name__ == '__main__':
    main()
