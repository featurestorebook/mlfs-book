import hopsworks
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta


def read_data_source(fs):
    fg = fs.get_feature_group("transactions", version=1)
    yesterday = datetime.now() - timedelta(days=1)
    return fg.filter(fg.ts > yesterday).read()

def pipeline(fs, df):
    fg2 = fs.get_feature_group("cc_aggs_trans", version=1)
    fg2.insert(df)

def main():
    fs = hopsworks.login().get_feature_store() 
    df = read_data_source(fs)
    pipeline(df)

def test_pipeline():
    fs = hopsworks.login().get_feature_store() 
    # Make sure the target feature group is empty for this test
    fg2 = fs.get_or_create_feature_group("cc_aggs_trans", version=1)
    fg2.delete()
    fg2 = fs.create_feature_group("cc_aggs_trans", version=1)

    df = pd.read_csv("sample_transactions.csv")
    pipeline(df)
    df2 = fg2.read()
    asset len(df) == len(df2) # Test correct number of rows were written

@patch('your_module.hopsworks')
def test_pipeline(mock_hopsworks):
    df = pd.read_csv("sample_transactions.csv")

    # Mock feature store and feature group
    mock_fs = MagicMock()
    mock_fg = MagicMock()
    mock_fs.get_feature_group.return_value = mock_fg
    mock_hopsworks.login.return_value.get_feature_store.return_value = mock_fs
    pipeline(mock_fs, df)

    # Assert that the insert method was called once with the dataframe
    mock_fg.insert.assert_called_once_with(df)

