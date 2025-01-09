import hopsworks
import pandas as pd
from math import radians
import numpy as np
from datetime import datetime


@hopsworks.udf(int, drop=['prev_transaction_time'])
def days_to_card_expiry(expiry_date: pd.Series) -> pd.Series:
    """
    """
    bins = [0, 1, 10, 30, 90, 180, float('inf')]
    labels = ['0-1', '2-10', '11-30', '31-90', '91-180', '181-']
    binned_days_to_expiry = pd.cut(expiry_date, bins=bins, labels=labels)
    return binned_days_to_expiry
    # return expiry_date.dt.date() - datetime.now.date()

@hopsworks.udf(int, hopsworks.training.Statistics)
def amount_deviation_from_avg(amount, statistics=statistics):
    """
    """
    return np.abs(amount - statistics.amount.mean)


@hopsworks.udf(int, drop=['prev_transaction_time'])
def time_since_last_trans(prev_transaction_time: pd.Series, transaction_time: pd.Series) -> pd.Series:
    """
    """
    return (transaction_time - prev_transaction_time).dt.total_seconds().astype(int)


@hopsworks.udf(int, drop=['prev_ip_addr'])
def haversine_distance(card_present, prev_card_present, 
                       ip_addr: pd.Series, prev_ip_addr: pd.Series,
                       time_since_last_trans: pd.Series) -> pd.Series:
    """
    """


    if card_present != True or prev_card_present != True :
        return 0
    
    latitude, longitude = # get from ip_addr
    prev_latitude, prev_longitude = # get from prev_ip_addr
    latitude_diff = latitude - prev_latitude
    longitude_diff = longitude - prev_longitude
    a = np.sin(latitude_diff/2.0)**2
    b = np.cos(latitude) * np.cos(prev_latitude) * np.sin(longitude_diff/2.0)**2
    c = 2*np.arcsin(np.sqrt(a + b))

    return c



def is_fraud(transactions_df: pd.DataFrame, cc_fraud_df: pd.DataFrame) -> pd.DataFrame:
    """
        if there is a label, set the value to be '1', otherwise '0'
    """
    labels_df = transactions_df.join(cc_fraud_df, on="transaction_id", how="left")
 
    # Make 'is_fraud' a label for identifying if a transaction is fraudulent or not
    labels_df = labels_df.withColumn("is_fraud", F.when(F.col("fraud_type").isNotNull(), 1).otherwise(0))

    # Drop cols that are not needed
    labels_df = labels_df.drop("fraud_report_id", "report_time", "fraud_type")

    return labels_df
