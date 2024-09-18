from transactions import get_transactions
from transactions import set_random_seed
from transactions import calculate_time_delta_t_minus_1, calculate_loc_delta_t_minus_1
import random
import hopsworks
import pandas as pd

random.seed()
seed = random.randint(0, 1000*1000*1000*2)
set_random_seed(seed)

project = hopsworks.login()
fs = project.get_feature_store()


trans_df, fraud_labels, profiles_df = get_transactions()
transactions_pdf = trans_df.merge(fraud_labels)

transactions_pdf['datetime'] = pd.to_datetime(transactions_pdf['datetime'])
transactions_pdf = transactions_pdf[["tid", "datetime", "cc_num", "category", "amount", "latitude", 'longitude', 'city', 'fraud_label']]
transactions_pdf.rename(columns={'cc_num': 'account_id'}, inplace=True)
transactions_pdf = calculate_time_delta_t_minus_1(transactions_pdf)
transactions_pdf = calculate_loc_delta_t_minus_1(transactions_pdf)

profiles_pdf = profiles_df
profiles_pdf['cc_expiration_date'] = pd.to_datetime(profiles_pdf['cc_expiration_date'])
profiles_pdf['birthdate'] = pd.to_datetime(profiles_pdf['birthdate'])
execution_timestamp = transactions_pdf['datetime'].min()
profiles_pdf['age'] = (execution_timestamp - profiles_pdf['birthdate']).dt.days / 365
profiles_pdf.drop('birthdate', axis=1, inplace=True)
profiles_pdf['cc_expiration_days'] = (profiles_pdf['cc_expiration_date'] - pd.Timestamp.now()).dt.days
profiles_pdf.drop('cc_expiration_date', axis=1, inplace=True)
profiles_pdf['event_time'] = execution_timestamp
profiles_pdf.rename(columns={'cc_num': 'account_id'}, inplace=True)

profiles_pdf = profiles_pdf[['account_id', 'cc_provider', 'cc_type', 'city', 'age', 'cc_expiration_days', 'event_time']]

profiles_pdf.to_csv('profs.csv')
transactions_pdf.to_csv("jim.csv")

transactions_pdf = transactions_pdf.merge(profiles_pdf[['account_id', 'city']], on='account_id')
transactions_pdf["outside_city"] = (transactions_pdf['city_x'] != transactions_pdf['city_y']).astype(int)

transactions_pdf.drop(['city_x', 'city_y', 'latitude', 'longitude'], axis=1, inplace=True)

transactions_pdf.to_csv("jim2.csv")

transactions_fg = fs.get_feature_group(name="transactions", version=1)
transactions_fg.insert(transactions_pdf)
profiles_fg = fs.get_feature_group("profiles", version=1)
profiles_fg.insert(profiles_pdf)
