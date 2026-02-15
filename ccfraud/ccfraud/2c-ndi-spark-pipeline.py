#!/usr/bin/env python3
"""
NDI (Net Disposable Income) Stability - Spark Batch Feature Pipeline

Reads from the account_ledger feature group, computes monthly net income
aggregations and rolling 12-month stability features, and writes the
results to the ndi_features_fg feature group.

Two stages:
  1. Monthly aggregation per account (~30-day buckets)
  2. Rolling 12-month window over monthly buckets
"""

import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

import hopsworks

# Setup paths and config
root_dir = Path().absolute()
if root_dir.parts[-1:] == ('notebooks',):
    root_dir = Path(*root_dir.parts[:-1])
if root_dir.parts[-1:] == ('ccfraud',):
    root_dir = Path(*root_dir.parts[:-1])
root_dir = str(root_dir)

sys.path.insert(0, root_dir)
sys.path.insert(0, str(Path(root_dir) / 'ccfraud'))

print(f'Root dir: {root_dir}')

from mlfs import config
settings = config.HopsworksSettings(_env_file=f'{root_dir}/.env')

project = hopsworks.login()
fs = project.get_feature_store()

# ---------------------------------------------------------------------------
# Step 1: Read source feature group
# ---------------------------------------------------------------------------
ledger_fg = fs.get_feature_group('account_ledger', version=1)
ledger_df = ledger_fg.read(dataframe_type='spark')

print(f'Account ledger: {ledger_df.count()} rows')
ledger_df.printSchema()

# ---------------------------------------------------------------------------
# Step 2: Monthly aggregation per account
# Bucket events into ~30-day months using FLOOR(unix_timestamp / 2592000)
# ---------------------------------------------------------------------------
SECONDS_PER_MONTH = 2592000  # 30 * 24 * 60 * 60

monthly_df = (
    ledger_df
    .withColumn('month_bucket', F.floor(F.unix_timestamp('event_time') / SECONDS_PER_MONTH))
    .groupBy('account_id', 'month_bucket')
    .agg(
        F.max('event_time').alias('event_time'),
        F.max('transaction_id').alias('transaction_id'),
        F.sum('credit').alias('total_credits'),
        F.sum('debit').alias('total_debits'),
        (F.sum('credit') - F.sum('debit')).alias('net_income'),
    )
)

print(f'Monthly buckets: {monthly_df.count()} rows')

# ---------------------------------------------------------------------------
# Step 3: Rolling 12-month window over monthly buckets
# ---------------------------------------------------------------------------
w12 = (
    Window.partitionBy('account_id')
    .orderBy('month_bucket')
    .rowsBetween(-11, 0)
)

ndi_df = (
    monthly_df
    .select(
        'account_id',
        'transaction_id',
        'event_time',
        F.col('net_income').alias('current_month_net'),
        F.sum('net_income').over(w12).alias('total_net_12m'),
        F.avg('net_income').over(w12).alias('avg_monthly_net_12m'),
        F.count('*').over(w12).alias('months_with_data'),
        F.max('net_income').over(w12).alias('max_monthly_net_12m'),
        F.min('net_income').over(w12).alias('min_monthly_net_12m'),
        (F.max('net_income').over(w12) - F.min('net_income').over(w12)).alias('ndi_range_12m'),
    )
)

print(f'NDI features: {ndi_df.count()} rows')
ndi_df.show(10)

# ---------------------------------------------------------------------------
# Step 4: Write to feature group
# ---------------------------------------------------------------------------
ndi_fg = fs.get_or_create_feature_group(
    name='ndi_features_fg',
    version=1,
    description='NDI stability features: rolling 12-month net income statistics per account',
    primary_key=['account_id'],
    event_time='event_time',
    online_enabled=True,
)

ndi_fg.insert(ndi_df, wait=True)
print('NDI Spark feature pipeline completed successfully!')
