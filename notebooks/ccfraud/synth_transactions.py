import polars as pl
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
from geopy.distance import geodesic
import random
import uuid
import hsfs
import hopsworks

# seeds
fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)


# ---------------------------
# Utility / fixed generators
# ---------------------------

def generate_merchant_details(rows: int, start_date: datetime, end_date: datetime) -> pl.DataFrame:
    """Generate merchant details DataFrame"""
    print("Generating merchant details...")
    if start_date is None or end_date is None:
        raise ValueError("start_date and end_date are required")

    categories = [
        "Groceries", "Travel", "Fashion", "Healthcare", "Restaurants", "Gas Stations",
        "Electronics", "Home Improvement", "Entertainment", "Education", "Insurance",
        "Automotive", "Books", "Sports", "Beauty", "Jewelry", "Pet Supplies",
        "Furniture", "Pharmacy", "Department Stores"
    ] * 25  # repeated to get many options

    countries = [
        "United States", "China", "Japan", "Germany", "India", "United Kingdom",
        "France", "Italy", "Brazil", "Canada", "Russia", "South Korea", "Australia",
        "Spain", "Mexico", "Indonesia", "Netherlands", "Saudi Arabia", "Turkey",
        "Taiwan", "Belgium", "Argentina", "Thailand", "Israel", "Poland", "Nigeria",
        # ... (shortened list for brevity, but you can expand as you wish)
    ]

    delta_days = max((end_date - start_date).days, 0)
    merchant_data = {
        "merchant_id": [f"MERCH_{i:05d}" for i in range(rows)],
        "category": [random.choice(categories) for _ in range(rows)],
        "country": [random.choice(countries) for _ in range(rows)],
        "cnt_chrgeback_prev_day": [round(np.random.exponential(2.5), 2) for _ in range(rows)],
        "cnt_chrgeback_prev_week": [None for _ in range(rows)], # computed in model-dependent transformation
        "cnt_chrgeback_prev_month": [None for _ in range(rows)], # computed in model-dependent transformation
        "last_modified": [start_date + timedelta(days=random.randint(0, delta_days)) for _ in range(rows)],
    }

    return pl.DataFrame(merchant_data)


def generate_bank_details(rows: int, start_date: datetime, end_date: datetime) -> pl.DataFrame:
    """Generate bank details DataFrame"""
    print("Generating bank details...")
    if start_date is None or end_date is None:
        raise ValueError("start_date and end_date are required")

    countries = ["United States", "China", "Japan", "Germany", "India", "United Kingdom",
                 "France", "Italy", "Brazil", "Canada", "Russia", "South Korea", "Australia",
                 "Spain", "Mexico", "Indonesia", "Netherlands", "Saudi Arabia", "Turkey"] * 7

    rows = int(rows)
    credit_ratings = np.random.beta(5, 2, rows) * 9 + 1
    credit_ratings = np.round(credit_ratings).astype(int)
    credit_ratings = np.clip(credit_ratings, 1, 10)

    delta_days = max((end_date - start_date).days, 0)
    bank_data = {
        "bank_id": [f"BANK_{i:05d}" for i in range(rows)],
        "country": [random.choice(countries) for _ in range(rows)],
        "credit_rating": credit_ratings.tolist(),
        "last_modified": [end_date - timedelta(days=random.randint(0, delta_days)) for _ in range(rows)],
        "days_since_bank_cr_changed": [None for _ in range(rows)], # computed in model-dependent transformation
    }

    return pl.DataFrame(bank_data)


def generate_account_details(
    rows: int ,
    account_creation_start_date: datetime,
    current_date: datetime,
    account_last_modified_start_date: datetime
) -> pl.DataFrame:
    """Generate account details DataFrame"""
    print("Generating account details...")
    if account_creation_start_date is None or current_date is None or account_last_modified_start_date is None:
        raise ValueError("Please provide account_creation_start_date, current_date and account_last_modified_start_date")

    rows = int(rows)
    delta_days_lm = max((current_date - account_last_modified_start_date).days, 0)
    account_data = {
        "account_id": [f"ACC_{i:06d}" for i in range(rows)],
        "name": [fake.name() for _ in range(rows)],
        "address": [fake.address().replace('\n', ', ') for _ in range(rows)],
        "debt_end_prev_month": [round(np.random.normal(2500, 1500), 2) for _ in range(rows)],
        "last_modified": [current_date - timedelta(days=random.randint(0, delta_days_lm)) for _ in range(rows)]
    }

    # creation & optional end dates
    creation_dates = []
    end_dates = []
    delta_days_creation = max((current_date - account_creation_start_date).days, 0)
    for _ in range(rows):
        creation = current_date - timedelta(days=random.randint(0, max(1, delta_days_creation)))
        creation_dates.append(creation)
        if random.random() < 0.9:
            end_dates.append(None)
        else:
            end_dates.append(creation + timedelta(days=random.randint(1, max(1, delta_days_creation))))

    account_data["creation_date"] = creation_dates
    account_data["end_date"] = end_dates

    return pl.DataFrame(account_data)


def generate_card_details(
    rows: int,
    num_accounts: int,
    num_banks: int,
    current_date: datetime,
    issue_date: datetime,
    expiry_date: datetime
) -> pl.DataFrame:
    """Generate card details DataFrame"""
    print("Generating card details...")
    if current_date is None or issue_date is None or expiry_date is None:
        raise ValueError("Provide current_date, issue_date and expiry_date")

    account_ids = [f"ACC_{i:06d}" for i in range(num_accounts)]
    bank_ids = [f"BANK_{i:05d}" for i in range(num_banks)]

    rows = int(rows)
    card_data = {
        "cc_num": [
            f"{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}-{random.randint(1000, 9999)}"
            for _ in range(rows)
        ],
        "account_id": [random.choice(account_ids) for _ in range(rows)],
        "bank_id": [random.choice(bank_ids) for _ in range(rows)],
    }

    delta_issue_days = max((current_date - issue_date).days, 0)
    expiry_days_span = max((expiry_date - current_date).days, 0)

    issue_dates = [(current_date - timedelta(days=random.randint(0, delta_issue_days))).replace(day=1) for _ in range(rows)]
    expiry_dates = [(current_date + timedelta(days=random.randint(0, expiry_days_span))).replace(day=1) for _ in range(rows)]

    card_data["issue_date"] = issue_dates
    card_data["cc_expiry_date"] = expiry_dates

    card_types = ["Credit"] * 60 + ["Debit"] * 35 + ["Prepaid"] * 5
    statuses = ["Active"] * 95 + ["Blocked"] * 4 + ["Lost/Stolen"] * 1
    card_data["card_type"] = [random.choice(card_types) for _ in range(rows)]
    card_data["status"] = [random.choice(statuses) for _ in range(rows)]

    card_data["last_modified"] = [current_date - timedelta(days=random.randint(0, delta_issue_days)) for _ in range(rows)]

    return pl.DataFrame(card_data)


# ---------------------------
# Transactions: From existing card + merchant (preserves FKs)
# ---------------------------

def generate_credit_card_transactions_from_existing(
    card_df: pl.DataFrame,
    merchant_df: pl.DataFrame,
    start_date: datetime,
    end_date: datetime,
    rows: int,
    tid_offset: int = 0,
    seed: int = 42,
) -> pl.DataFrame:
    """
    Create transactions by sampling existing card_df (cc_num, account_id) and merchant_df (merchant_id).
    Returns a Polars DataFrame with columns: t_id, cc_num, account_id, merchant_id, amount, ip_address, card_present, ts
    """
    print("Generating credit card transactions from existing card + merchant tables...")
    rows = int(rows)
    rng = np.random.default_rng(seed)

    # Ensure required columns exist
    required_card_cols = {"cc_num", "account_id"}
    if not required_card_cols.issubset(set(card_df.columns)):
        raise ValueError(f"card_df must contain columns: {required_card_cols}")

    if "merchant_id" not in merchant_df.columns:
        raise ValueError("merchant_df must contain 'merchant_id' column")

    # 1) Sample (cc_num, account_id)
    # Polars sampling with replacement
    cards_sample = card_df.select(["cc_num", "account_id"]).sample(n=rows, with_replacement=True, shuffle=True, seed=seed)
    merchants_sample = merchant_df.select(["merchant_id"]).sample(n=rows, with_replacement=True, shuffle=True, seed=seed + 1)

    # 2) Amounts (log-normal) and card_present flag
    amounts = np.round(rng.lognormal(mean=3.5, sigma=1.2, size=rows), 2).tolist()
    card_present = rng.integers(0, 2, size=rows).astype(bool).tolist()

    # 3) IP addresses (random)
    a = rng.integers(1, 256, size=rows)
    b = rng.integers(0, 256, size=rows)
    c = rng.integers(0, 256, size=rows)
    d = rng.integers(0, 256, size=rows)
    ip_address = [f"{int(x)}.{int(y)}.{int(z)}.{int(w)}" for x, y, z, w in zip(a, b, c, d)]

    # 4) timestamps uniform in [start_date, end_date)
    total_seconds = max(int((end_date - start_date).total_seconds()), 1)
    offsets = rng.integers(0, total_seconds, size=rows)
    ts = [start_date + timedelta(seconds=int(s)) for s in offsets]

    # 5) t_id
    t_ids = list(range(tid_offset, tid_offset + rows))

    # Build DataFrame using columns from sampled frames (they are Polars Series)
    df = pl.DataFrame({
        "t_id": t_ids,
        "cc_num": cards_sample["cc_num"].to_list(),
        "account_id": cards_sample["account_id"].to_list(),
        "merchant_id": merchants_sample["merchant_id"].to_list(),
        "amount": amounts,
        "ip_address": ip_address,
        "card_present": card_present,
        "ts": ts
    })

    return df


# ---------------------------
# Feature group helper - add account_id description
# ---------------------------

def create_feature_group_with_descriptions(fs, df, name, description, primary_key, event_time_col=None, topic_name=None, online_enabled=True):
    """Create feature group and add feature descriptions"""
    print(f"Creating feature group: {name}")
    if topic_name is None:
        try:
            topic_name = fs.name.removesuffix("_featurestore")
        except Exception:
            topic_name = None

    fg = fs.create_feature_group(
        name=name,
        version=1,
        description=description,
        primary_key=primary_key,
        event_time=event_time_col,
        topic_name=topic_name,
        online_enabled=online_enabled
    )

    fg.insert(df)

    feature_descriptions = {
        "merchant_details": {
            "merchant_id": "Unique identifier for each merchant",
            "category": "Merchant category codes for goods or service purchased",
            "country": "Country where merchant resides",
            "cnt_chrgeback_prev_day": "Number of chargebacks for this merchant during the previous day",
            "cnt_chrgeback_prev_week": "Number of chargebacks for this merchant during the previous week",
            "cnt_chrgeback_prev_month": "Number of chargebacks for this merchant during the previous month",
            "last_modified": "Timestamp when the merchant details was last updated"
        },
        "bank_details": {
            "bank_id": "Unique identifier for each bank",
            "country": "Country where bank resides",
            "credit_rating": "Bank credit rating on a scale of 1 to 10",
            "last_modified": "Timestamp when the bank details was last updated"
        },
        "account_details": {
            "account_id": "Unique identifier for the card owner",
            "name": "Full name of the account owner",
            "address": "Address where account owner resides",
            "debt_end_prev_month": "Amount of debt/credit at end of previous month",
            "last_modified": "Timestamp when the account_details was last updated",
            "creation_date": "Timestamp when the account was created",
            "end_date": "Timestamp when the account was closed (null if still active)"
        },
        "card_details": {
            "cc_num": "Credit card number in format XXXX-XXXX-XXXX-XXXX",
            "cc_expiry_date": "Card's expiration date (year and month only)",
            "account_id": "Foreign key reference to account_details table",
            "bank_id": "Foreign key reference to bank_details table",
            "issue_date": "Card's issue date (0 to 3 years before current date)",
            "card_type": "Type of card (Credit, Debit, Prepaid)",
            "status": "Current status of the card (Active, Blocked, Lost/Stolen)",
            "last_modified": "Timestamp when the card details was last updated"
        },
        "credit_card_transactions": {
            "t_id": "Unique identifier for this credit card transaction",
            "cc_num": "Foreign key reference to credit card (card_details.cc_num)",
            "account_id": "Foreign key reference to account_details (via card_details.account_id)",
            "merchant_id": "Foreign key reference to merchant table",
            "amount": "Credit card transaction amount in decimal format",
            "ip_address": "IP address of the physical or online merchant (format: XXX.XXX.XXX.XXX)",
            "card_present": "Whether credit card was used in a physical terminal (true) or online payment (false)",
            "ts": "Timestamp for this credit card transaction"
        }
        "cc_fraud": {
            "t_id": "Unique identifier for this credit card transaction",
            "cc_num": "Foreign key reference to credit card (card_details.cc_num)",
            "explanation": "Reasoning for why the Credit card transaction was marked as fraudulent (e.g., geographic ",
            "ts": "Timestamp for this credit card transaction"
        }
    }

    if name in feature_descriptions:
        for feature_name, feature_desc in feature_descriptions[name].items():
            if feature_name in df.columns:
                try:
                    fg.update_feature_description(feature_name=feature_name, description=feature_desc)
                    print(f"  Added description for: {feature_name}")
                except Exception as e:
                    print(f"  Warning: Could not add description for {feature_name}: {e}")

    return fg


