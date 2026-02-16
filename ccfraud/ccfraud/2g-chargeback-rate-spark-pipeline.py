#!/usr/bin/env python3
"""
Chargeback Rate Feature Pipeline - Spark Batch

Reads credit_card_transactions and disputed_transactions feature groups,
joins them, and computes per (merchant_id, card_entry_mode) chargeback
rates at 1-day, 7-day, and 30-day rolling windows.

Steps:
  1. Read credit_card_transactions and disputed_transactions FGs
  2. Add card_entry_mode to all transactions
  3. Left join disputes to flag disputed transactions
  4. Day-bucket all rows
  5. Aggregate daily counts per (merchant_id, card_entry_mode)
  6. Compute rolling chargeback rates (1d, 7d, 30d)
  7. Write to chargeback_rate_features_fg

Requires:
  - credit_card_transactions feature group (run datamart first)
  - disputed_transactions feature group (run disputes-datamart first)

Usage:
    python ccfraud/2g-chargeback-rate-spark-pipeline.py
"""

import hopsworks
from pyspark.sql import functions as F
from pyspark.sql import Window

project = hopsworks.login()
fs = project.get_feature_store()

# ---------------------------------------------------------------------------
# Step 1: Read source feature groups
# ---------------------------------------------------------------------------
txn_fg = fs.get_feature_group('credit_card_transactions', version=1)
disputes_fg = fs.get_feature_group('disputed_transactions', version=1)

txn_df = txn_fg.read(dataframe_type='spark')
disputes_df = disputes_fg.read(dataframe_type='spark')

print(f'credit_card_transactions: {txn_df.count()} rows')
print(f'disputed_transactions:    {disputes_df.count()} rows')

# ---------------------------------------------------------------------------
# Step 2: Add card_entry_mode to all transactions
# 'swiped' if card_present == True, 'keyed' if False
# ---------------------------------------------------------------------------
txn_df = txn_df.withColumn(
    'card_entry_mode',
    F.when(F.col('card_present') == True, F.lit('swiped'))
     .otherwise(F.lit('keyed')),
)

# ---------------------------------------------------------------------------
# Step 3: Left join disputes to flag disputed transactions
# ---------------------------------------------------------------------------
disputes_flag = (
    disputes_df
    .select(F.col('t_id').alias('d_t_id'))
    .distinct()
    .withColumn('is_disputed', F.lit(1))
)

txn_df = (
    txn_df
    .join(disputes_flag, txn_df['t_id'] == disputes_flag['d_t_id'], 'left')
    .drop('d_t_id')
    .withColumn('is_disputed', F.coalesce(F.col('is_disputed'), F.lit(0)))
)

# ---------------------------------------------------------------------------
# Step 4: Day-bucket
# ---------------------------------------------------------------------------
SECONDS_PER_DAY = 86400

txn_df = txn_df.withColumn(
    'day_bucket', F.floor(F.unix_timestamp('ts') / SECONDS_PER_DAY)
)

# ---------------------------------------------------------------------------
# Step 5: Aggregate daily counts per (merchant_id, card_entry_mode)
# ---------------------------------------------------------------------------
daily = (
    txn_df
    .groupBy('merchant_id', 'card_entry_mode', 'day_bucket')
    .agg(
        F.count('*').alias('daily_txn_count'),
        F.sum('is_disputed').alias('daily_dispute_count'),
    )
)

# Reconstruct event_time from day_bucket (start of day)
daily = daily.withColumn(
    'event_time',
    F.to_timestamp(F.from_unixtime(F.col('day_bucket') * SECONDS_PER_DAY)),
)

# ---------------------------------------------------------------------------
# Step 6: Rolling chargeback rates (1d, 7d, 30d)
# ---------------------------------------------------------------------------
def rolling_window(days):
    return (
        Window.partitionBy('merchant_id', 'card_entry_mode')
        .orderBy('day_bucket')
        .rowsBetween(-days + 1, 0)
    )

w1 = rolling_window(1)
w7 = rolling_window(7)
w30 = rolling_window(30)

result_df = (
    daily
    .withColumn('sum_txn_1d', F.sum('daily_txn_count').over(w1))
    .withColumn('sum_dis_1d', F.sum('daily_dispute_count').over(w1))
    .withColumn('sum_txn_7d', F.sum('daily_txn_count').over(w7))
    .withColumn('sum_dis_7d', F.sum('daily_dispute_count').over(w7))
    .withColumn('sum_txn_30d', F.sum('daily_txn_count').over(w30))
    .withColumn('sum_dis_30d', F.sum('daily_dispute_count').over(w30))
    .withColumn(
        'chargeback_rate_1d',
        F.when(F.col('sum_txn_1d') > 0,
               F.col('sum_dis_1d') / F.col('sum_txn_1d'))
         .otherwise(F.lit(0.0)),
    )
    .withColumn(
        'chargeback_rate_7d',
        F.when(F.col('sum_txn_7d') > 0,
               F.col('sum_dis_7d') / F.col('sum_txn_7d'))
         .otherwise(F.lit(0.0)),
    )
    .withColumn(
        'chargeback_rate_30d',
        F.when(F.col('sum_txn_30d') > 0,
               F.col('sum_dis_30d') / F.col('sum_txn_30d'))
         .otherwise(F.lit(0.0)),
    )
)

# ---------------------------------------------------------------------------
# Step 7: Select final columns and write to feature group
# ---------------------------------------------------------------------------
result_df = result_df.select(
    'merchant_id',
    'card_entry_mode',
    'event_time',
    'chargeback_rate_1d',
    'chargeback_rate_7d',
    'chargeback_rate_30d',
)

print(f'\nChargeback rate features: {result_df.count()} rows')
result_df.show(10)

# Write to Hopsworks
chargeback_fg = fs.get_or_create_feature_group(
    name='chargeback_rate_features_fg',
    version=1,
    description=(
        'Per-merchant, per-card-entry-mode chargeback rates at 1-day, 7-day, '
        'and 30-day rolling windows. Joins credit_card_transactions with '
        'disputed_transactions.'
    ),
    primary_key=['merchant_id', 'card_entry_mode'],
    event_time='event_time',
    online_enabled=True,
)

chargeback_fg.insert(result_df, wait=True)
print('Chargeback rate Spark feature pipeline completed successfully!')
