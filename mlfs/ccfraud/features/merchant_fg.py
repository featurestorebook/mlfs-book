import hopsworks 
import pandas as pd

def count_chargeback_rate_prev_month(chargeback_prev_day: pd.Series) -> pd.Series:
    """
    """
    merchant_df = common.fraud_rate_by_num_days("merchant_id", merchant_df, 7)
    return merchant_df

def count_chargeback_rate_prev_week(merchant_df: pd.Series) -> pd.Series:
    """
    """
    merchant_df = common.fraud_rate_by_num_days("merchant_id", merchant_df, 30)
    return merchant_df

def count_chargeback_rate_prev_day(location: pd.Series) -> pd.Series:
    """
    """
    merchant_df = common.fraud_rate_by_num_days("merchant_id", merchant_df, 1)
    return merchant_df
