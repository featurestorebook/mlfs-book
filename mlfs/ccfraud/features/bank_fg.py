import hopsworks
import pandas as pd
from datetime import datetime



def days_since_bank_cr_changed(df: pd.DataFrame) -> pd.DataFrame:
    """
    """
    df['days_since_bank_cr_changed'] = df['today'] - df['days_since_bank_cr_changed']
    return df