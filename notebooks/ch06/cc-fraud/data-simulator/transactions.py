#!/usr/bin/env python
# coding: utf-8
# %%
# pip install faker

#  Dataset simulator

from collections import defaultdict
from faker import Faker
import pandas as pd
import numpy as np
import datetime
import hashlib
import random
import math
import bisect
from typing import Optional, Union, Any, Dict, List, TypeVar, Tuple
import json

from math import radians
import numpy as np
import pandas as pd
from typing import Union

# Seed for Reproducibility
faker = Faker()
faker.seed_locale('en_US', 0)


def set_random_seed(seed: int):
    random.seed(seed)
    np.random.seed(seed)
    faker.seed_instance(seed)


set_random_seed(12345)

TOTAL_UNIQUE_USERS = 1000
TOTAL_UNIQUE_TRANSACTIONS = 54000
CASH_WITHRAWAL_CARDS_TOTAL = 2000
TOTAL_UNIQUE_CASH_WITHDRAWALS = 1200
ATM_WITHRAWAL_SEQ_LENGTH = [3, 4, 5, 6, 7, 8, 9, 10]
NORMAL_ATM_RADIUS = 0.01

DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

date_time = datetime.datetime.now()
if date_time.tzinfo is None:
    date_time = date_time.replace(tzinfo=datetime.timezone.utc)
END_DATE = date_time.strftime(DATE_FORMAT)
START_DATE = (date_time - datetime.timedelta(days=30 * 6)).strftime(DATE_FORMAT)

AMOUNT_DISTRIBUTION_PERCENTAGES = {
    0.05: (0.01, 1.01),
    0.075: (1, 11.01),
    0.525: (10, 100.01),
    0.25: (100, 1000.01),
    0.099: (1000, 10000.01),
    0.001: (10000, 30000.01)
}

CATEGORY_PERC_PRICE = {
    "Grocery": (0.5, 0.01, 100),
    "Restaurant/Cafeteria": (0.2, 1, 100),
    "Health/Beauty": (0.1, 10, 500.01),
    "Domestic Transport": (0.1, 10, 100.01),
    "Clothing": (0.05, 10, 2000.01),
    "Electronics": (0.02, 100, 10000.01),
    "Sports/Outdoors": (0.015, 10, 100.01),
    "Holliday/Travel": (0.014, 10, 100.01),
    "Jewelery": (0.001, 10, 100.01)
}

FRAUD_RATIO = 0.0025  # percentage of transactions that are fraudulent
NUMBER_OF_FRAUDULENT_TRANSACTIONS = int(FRAUD_RATIO * TOTAL_UNIQUE_TRANSACTIONS)
ATTACK_CHAIN_LENGTHS = [3, 4, 5, 6, 7, 8, 9, 10]

SUSCEPTIBLE_CARDS_DISTRIBUTION_BY_AGE = {
    0.055: (17, 24),
    0.0015: (24, 34),
    0.0015: (34, 44),
    0.02: (44, 54),
    0.022: (54, 64),
    0.1: (64, 74),
    0.40: (74, 84),
    0.40: (84, 100),
}


def date_to_year_month(date_obj: datetime) -> datetime.date:
    return date_obj.strftime('%Y-%m')


def generate_unique_credit_card_numbers(n: int) -> pd.Series:
    """."""
    cc_ids = set()
    for _ in range(n):
        cc_id = faker.credit_card_number(card_type='visa')
        cc_ids.add(cc_id)
    return pd.Series(list(cc_ids))


# write a pytest - assert len(credit_card_numbers) == TOTAL_UNIQUE_USERS
# assert len(credit_card_numbers[0]) == 16 # validate if generated number is 16-digit

def generate_list_credit_card_numbers() -> list:
    """."""
    credit_cards = []
    credit_card_numbers = generate_unique_credit_card_numbers(TOTAL_UNIQUE_USERS)
    date_time = datetime.datetime.now()
    if date_time.tzinfo is None:
        date_time = date_time.replace(tzinfo=datetime.timezone.utc)
    delta_time_object = date_time.strptime(START_DATE, DATE_FORMAT)
    delta_time_object + datetime.timedelta(days=-728)
    for cc_num in credit_card_numbers:
        hexdigest = hashlib.md5(cc_num.encode('utf-8')).hexdigest()
        credit_cards.append({'cc_num': hexdigest, 'provider': random.choice(['visa', 'mastercard']),
                             'expires': faker.credit_card_expire(start=delta_time_object, end="+5y",
                                                                 date_format="%m/%y")})
    return credit_cards


def generate_df_with_profiles(credit_cards: list) -> pd.DataFrame:
    """."""
    profiles = []
    for credit_card in credit_cards:
        address = faker.local_latlng(country_code='US')
        age = 0
        profile = None
        while age < 18 or age > 100:
            profile = faker.profile(fields=['name', 'mail', 'birthdate'])
            dday = profile['birthdate']
            delta = datetime.datetime.now() - datetime.datetime(dday.year, dday.month, dday.day)
            age = int(delta.days / 365)
        profile['city'] = address[2]
        profile['country_of_residence'] = address[3]
        profile['cc_num'] = credit_card['cc_num']
        profile['cc_provider'] = credit_card['provider']
        profile['cc_type'] = random.choice(["credit", "debit"])
        profile['cc_expiration_date'] = credit_card['expires']
        credit_card['age'] = age
        profiles.append(profile)

    # Cast the columns to the correct Pandas DType
    profiles_df = pd.DataFrame.from_records(profiles)
    profiles_df['birthdate'] = pd.to_datetime(profiles_df['birthdate'])
    profiles_df['cc_expiration_date'] = pd.to_datetime(profiles_df['cc_expiration_date'], format="%m/%y")
    # profiles_df['cc_num'] = pd.to_numeric(profiles_df['cc_num'])

    return profiles_df


#  pyasset - assert len(timestamps) == TOTAL_UNIQUE_TRANSACTIONS
def generate_timestamps(n: int) -> list:
    """Return a list of timestamps of length 'n'."""
    start = datetime.datetime.strptime(START_DATE, DATE_FORMAT)
    end = datetime.datetime.strptime(END_DATE, DATE_FORMAT)
    timestamps = list()
    for _ in range(n):
        timestamp = faker.date_time_between(start_date=start, end_date=end, tzinfo=None).strftime(DATE_FORMAT)
        timestamps.append(timestamp)
    timestamps = sorted(timestamps)
    return timestamps


def get_random_transaction_amount(start: float, end: float) -> float:
    """."""
    amt = round(np.random.uniform(start, end), 2)
    return amt


def generate_amounts() -> list:
    """."""
    amounts = []
    for percentage, span in AMOUNT_DISTRIBUTION_PERCENTAGES.items():
        n = int(TOTAL_UNIQUE_TRANSACTIONS * percentage)
        start, end = span
        for _ in range(n):
            amounts.append(get_random_transaction_amount(start, end + 1))
    return amounts


def generate_categories(amounts) -> list:
    """."""
    categories = []
    for category, category_perc_price in CATEGORY_PERC_PRICE.items():
        percentage, min_price, max_price = category_perc_price
        n = int(TOTAL_UNIQUE_TRANSACTIONS * percentage)
        for _ in range(n):
            min_price_i = bisect.bisect_left(amounts, min_price)
            max_price_i = bisect.bisect_right(amounts, max_price, lo=min_price_i)
            categories.append({"category": category, "amount": random.choice(amounts[min_price_i:max_price_i])})

    random.shuffle(categories)
    return categories


def generate_transaction_id(timestamp: str, credit_card_number: str, transaction_amount: float) -> str:
    """."""
    hashable = f'{timestamp}{credit_card_number}{transaction_amount}'
    hexdigest = hashlib.md5(hashable.encode('utf-8')).hexdigest()
    return hexdigest


def generate_transactions(credit_card_numbers: list, timestamps: list, categories: list) -> list:
    """."""
    transactions = []
    for timestamp, category in zip(timestamps, categories):
        credit_card_number = random.choice(credit_card_numbers)
        point_of_tr = faker.local_latlng(country_code='US')
        transaction_id = generate_transaction_id(timestamp, credit_card_number, category['amount'])
        transactions.append({
            'tid': transaction_id,
            'datetime': timestamp,
            'cc_num': credit_card_number,
            'category': category['category'],
            'amount': category['amount'],
            'latitude': point_of_tr[0],
            'longitude': point_of_tr[1],
            'city': point_of_tr[2],
            'country': point_of_tr[3],
            'fraud_label': 0
        }
        )
    return transactions


def generate_cash_amounts() -> list:
    """."""
    cash_amounts = []
    for percentage, span in AMOUNT_DISTRIBUTION_PERCENTAGES.items():
        n = int(TOTAL_UNIQUE_CASH_WITHDRAWALS * percentage)
        start, end = span
        for _ in range(n):
            cash_amounts.append(get_random_transaction_amount(start, end + 1))
    return cash_amounts


def generate_chains():
    """."""
    visited = set()
    chains = defaultdict(list)

    def size(chains: dict) -> int:
        counts = {key: len(values) + 1 for (key, values) in chains.items()}
        return sum(counts.values())

    def generate_attack_chain(i: int):
        chain_length = random.choice(ATTACK_CHAIN_LENGTHS)
        for j in range(1, chain_length):
            if i + j not in visited:
                if size(chains) == NUMBER_OF_FRAUDULENT_TRANSACTIONS:
                    break
                chains[i].append(i + j)
                visited.add(i + j)

    while size(chains) < NUMBER_OF_FRAUDULENT_TRANSACTIONS:
        i = random.choice(range(TOTAL_UNIQUE_TRANSACTIONS))
        if i not in visited:
            generate_attack_chain(i)
            visited.add(i)
    return chains


def generate_atm_withdrawal(credit_card_number: str, cash_amounts: list, length: int, \
                            delta: int, radius: float = None, country_code='US') -> List[Dict]:
    """."""
    atms = []
    start = datetime.datetime.strptime(START_DATE, DATE_FORMAT)
    end = datetime.datetime.strptime(END_DATE, DATE_FORMAT)
    timestamp = faker.date_time_between(start_date=start, end_date=end, tzinfo=None)
    point_of_tr = faker.local_latlng(country_code=country_code)
    latitude = point_of_tr[0]
    longitude = point_of_tr[1]
    city = point_of_tr[2]
    for _ in range(length):
        current = timestamp - datetime.timedelta(hours=delta)
        if radius is not None:
            latitude = faker.coordinate(latitude, radius)
            longitude = faker.coordinate(longitude, radius)
        amount = random.sample(cash_amounts, 1)[0]
        transaction_id = generate_transaction_id(timestamp, credit_card_number, amount)
        atms.append({'tid': transaction_id,
                     'datetime': current.strftime(DATE_FORMAT),
                     'cc_num': credit_card_number,
                     'category': 'Cash Withdrawal',
                     'amount': amount,
                     'latitude': latitude,
                     'longitude': longitude,
                     'city': city,
                     'country': 'US',
                     'fraud_label': 0
                     })
        timestamp = current
    return atms


def generate_susceptible_cards(credit_cards: list) -> list:
    """."""
    susceptible_cards = []
    visited_cards = []
    for percentage, span in SUSCEPTIBLE_CARDS_DISTRIBUTION_BY_AGE.items():
        n = int(TOTAL_UNIQUE_CASH_WITHDRAWALS * percentage)
        start, end = span
        for _ in range(n):
            for card in credit_cards:
                if card['age'] > start and card['age'] < end:
                    if card['cc_num'] not in visited_cards:
                        current = card
                        visited_cards.append(card['cc_num'])
                        break
                    else:
                        current = None
            if current is not None:
                susceptible_cards.append(current)
    return susceptible_cards


def generate_normal_atm_withdrawals(cash_amounts: list, susceptible_cards: list) -> list:
    """."""
    normal_atm_withdrawals = []
    atm_transactions = len(cash_amounts)
    cash_withdrawal_cards = random.sample(susceptible_cards, CASH_WITHRAWAL_CARDS_TOTAL // (
            CASH_WITHRAWAL_CARDS_TOTAL // len(susceptible_cards) + 1))
    atm_count = 0
    while atm_count < atm_transactions:
        for card in cash_withdrawal_cards:
            for ATM_WITHRAWAL_SEQ in ATM_WITHRAWAL_SEQ_LENGTH:
                # interval in hours between normal cash withdrawals
                delta = random.randint(6, 168)
                atm_tr = generate_atm_withdrawal(credit_card_number=card['cc_num'], cash_amounts=cash_amounts,
                                                 length=ATM_WITHRAWAL_SEQ, delta=delta, radius=NORMAL_ATM_RADIUS)
                normal_atm_withdrawals.append(atm_tr)
                atm_count += ATM_WITHRAWAL_SEQ
    return normal_atm_withdrawals


def generate_timestamps_for_fraud_attacks(timestamp: str, chain_length: int) -> list:
    """."""
    timestamps = []
    timestamp = datetime.datetime.strptime(timestamp, DATE_FORMAT)
    for _ in range(chain_length):
        # interval in seconds between fraudulent attacks
        delta = random.randint(30, 120)
        current = timestamp + datetime.timedelta(seconds=delta)
        timestamps.append(current.strftime(DATE_FORMAT))
        timestamp = current
    return timestamps


def generate_amounts_for_fraud_attacks(chain_length: int) -> list:
    """."""
    amounts = []
    for percentage, span in AMOUNT_DISTRIBUTION_PERCENTAGES.items():
        n = math.ceil(chain_length * percentage)
        start, end = span
        for _ in range(n):
            amounts.append(get_random_transaction_amount(start, end + 1))
    return amounts[:chain_length]


def update_transactions(transactions: list, chains: list) -> list:
    """."""
    for key, chain in chains.items():
        transaction = transactions[key]
        timestamp = transaction['datetime']
        cc_num = transaction['cc_num']
        amount = transaction['amount']
        transaction['fraud_label'] = 1
        inject_timestamps = generate_timestamps_for_fraud_attacks(timestamp, len(chain))
        inject_amounts = generate_amounts_for_fraud_attacks(len(chain))
        random.shuffle(inject_amounts)
        for i, idx in enumerate(chain):
            original_transaction = transactions[idx]
            inject_timestamp = inject_timestamps[i]
            original_transaction['datetime'] = inject_timestamp
            original_transaction['fraud_label'] = 1
            original_transaction['cc_num'] = cc_num
            original_transaction['amount'] = inject_amounts[i]
            original_transaction['category'] = \
                [category for category, category_perc_price in CATEGORY_PERC_PRICE.items() if
                 int(inject_amounts[i]) in range(int(category_perc_price[1]), int(category_perc_price[2]))][0]
            original_transaction['tid'] = generate_transaction_id(inject_timestamp, cc_num, amount)
            transactions[idx] = original_transaction


def generate_fraudulent_atm_tr_indxs(normal_atm_withdrawals: list) -> list:
    """."""
    return random.sample([i for i in range(0, len(normal_atm_withdrawals))], \
                         int(FRAUD_RATIO * len(normal_atm_withdrawals)))


def update_normal_atm_withdrawals(fraudulent_atm_tr_indxs: list, normal_atm_withdrawals: list, \
                                  cash_amounts: list):
    """."""
    for fraudulent_atm_tr_indx in fraudulent_atm_tr_indxs:
        # interval in seconds between fraudulent attacks
        delta = random.randint(1, 5)
        atm_withdrawal = normal_atm_withdrawals[fraudulent_atm_tr_indx]
        pre_fraudulent_atm_tr = atm_withdrawal[0]
        fraudulent_atm_tr = generate_atm_withdrawal(credit_card_number=
                                                    pre_fraudulent_atm_tr['cc_num'], cash_amounts=cash_amounts,
                                                    length=1, delta=delta, radius=None)[0]
        fraudulent_atm_location = faker.location_on_land()
        while fraudulent_atm_location[3] == 'US':
            fraudulent_atm_location = faker.location_on_land()

        fraudulent_atm_tr['datetime'] = (datetime.datetime.strptime(pre_fraudulent_atm_tr['datetime'],
                                                                    DATE_FORMAT) + datetime.timedelta(
            hours=delta)).strftime(DATE_FORMAT)

        fraudulent_atm_tr['latitude'] = fraudulent_atm_location[0]
        fraudulent_atm_tr['longitude'] = fraudulent_atm_location[1]
        fraudulent_atm_tr['city'] = fraudulent_atm_location[2]
        fraudulent_atm_tr['country'] = fraudulent_atm_location[3]
        fraudulent_atm_tr['fraud_label'] = 1
        atm_withdrawal.append(fraudulent_atm_tr)
        normal_atm_withdrawals[fraudulent_atm_tr_indx] = atm_withdrawal


def transactions_as_dataframe(transactions: list, normal_atm_withdrawals: list) -> pd.DataFrame:
    """."""
    for atm_withdrawal in normal_atm_withdrawals:
        for withdrawal in atm_withdrawal:
            transactions.append(withdrawal)
    return pd.DataFrame.from_records(transactions)


def create_credit_cards_as_df(credit_cards: list) -> pd.DataFrame:
    """."""
    df = pd.DataFrame.from_records(credit_cards)
    return df


def create_profiles_as_df(credit_cards: list) -> pd.DataFrame:
    """."""
    profiles_df = generate_df_with_profiles(credit_cards)
    return profiles_df


def create_transactions_as_df(credit_cards: list) -> pd.DataFrame:
    """."""
    timestamps = generate_timestamps(TOTAL_UNIQUE_TRANSACTIONS)
    amounts = generate_amounts()
    categories = generate_categories(amounts)
    cc_df = create_credit_cards_as_df(credit_cards)
    transactions = generate_transactions(cc_df['cc_num'], timestamps, categories)
    cash_amounts = generate_cash_amounts()
    chains = generate_chains()
    susceptible_cards = generate_susceptible_cards(credit_cards)

    normal_atm_withdrawals = generate_normal_atm_withdrawals(cash_amounts, susceptible_cards)
    update_transactions(transactions, chains)
    fraudulent_atm_tr_indxs = generate_fraudulent_atm_tr_indxs(normal_atm_withdrawals)
    update_normal_atm_withdrawals(fraudulent_atm_tr_indxs, normal_atm_withdrawals, cash_amounts)

    transactions_df = transactions_as_dataframe(transactions, normal_atm_withdrawals)

    # Cast the columns to the correct Pandas DType
    transactions_df['longitude'] = pd.to_numeric(transactions_df['longitude'])
    transactions_df['latitude'] = pd.to_numeric(transactions_df['latitude'])

    fraud_labels = transactions_df[["tid", "cc_num", "fraud_label", "datetime"]]
    transactions_df = transactions_df.drop(columns=["fraud_label"])
    return transactions_df, fraud_labels


# get dataset
def get_transactions():
    credit_cards = generate_list_credit_card_numbers()
    profiles_df = create_profiles_as_df(credit_cards)
    trans_df, fraud_labels = create_transactions_as_df(credit_cards)
    return trans_df, fraud_labels, profiles_df


def simulate_live_transactions():
    trans_df, _, _ = get_transactions()
    trans_df = trans_df[['tid', 'datetime', 'amount', 'cc_num']]
    trans_df['json'] = trans_df.apply(lambda x: x.to_json(), axis=1)
    return [json.loads(i) for i in trans_df.json.values]



def haversine(long: pd.Series, lat: pd.Series, shift: int) -> np.ndarray:
    """
    Compute Haversine distance between each consecutive coordinate in (long, lat).

    Parameters:
    - long: pandas Series, longitude values
    - lat: pandas Series, latitude values
    - shift: int, the number of positions to shift for calculating distances

    Returns:
    - numpy array, Haversine distances
    """
    long_shifted = long.shift(shift)
    lat_shifted = lat.shift(shift)
    long_diff = long_shifted - long
    lat_diff = lat_shifted - lat

    a = np.sin(lat_diff/2.0)**2
    b = np.cos(lat) * np.cos(lat_shifted) * np.sin(long_diff/2.0)**2
    c = 2*np.arcsin(np.sqrt(a + b))

    return c

def calculate_loc_delta_t_minus_1(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate loc_delta_t_minus_1 for each group.

    Parameters:
    - group: pandas DataFrame group, grouped by 'cc_num'

    Returns:
    - pandas Series, loc_delta_t_minus_1 values
    """
    # Sort values and convert latitude and longitude to radians
    df = df.sort_values("datetime")
    df[["longitude", "latitude"]] = df[["longitude", "latitude"]].applymap(radians)

    df["loc_delta_t_minus_1"] = df.groupby("account_id").apply(
        lambda x: haversine(x["longitude"], x["latitude"], +1)
    ).reset_index(level=0, drop=True).fillna(0)

    return df


def time_delta(datetime_value: pd.Series, shift: int) -> pd.Series:
    """
    Compute time difference between each consecutive transaction.

    Parameters:
    - datetime_value: pandas Series, datetime values
    - shift: int, the number of positions to shift for calculating time differences

    Returns:
    - pandas Series, time differences
    """
    time_shifted = datetime_value.shift(shift)
    return time_shifted




def calculate_time_delta_t_minus_1(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate time_delta_t_minus_1 for each group.

    Parameters:
    - group: pandas DataFrame group, grouped by 'account_id'

    Returns:
    - pandas Series, time_delta_t_minus_1 values
    """
    df = df.sort_values("datetime")
    df["time_delta_t_minus_1"] = df.groupby("account_id") \
        .apply(lambda x: time_delta(x["datetime"], +1))\
        .reset_index(level=0, drop=True)

    # Normalize time_delta_t_minus_1 to days and handle missing values
    df["time_delta_t_minus_1"] = (df["datetime"] - df["time_delta_t_minus_1"]) / np.timedelta64(1, 'D')
    df["time_delta_t_minus_1"] = df["time_delta_t_minus_1"].fillna(0)
    return df
