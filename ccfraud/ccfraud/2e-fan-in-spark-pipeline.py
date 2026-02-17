#!/usr/bin/env python3
"""
Fan-In Detection - Spark Batch Feature Pipeline

Reads from the money_transfers feature group, computes fan-in structuring
features via a self-join, and writes the results to the fan_in_features_fg
feature group.

Detects "fan-in" structuring patterns: multiple distinct senders sending
near-threshold amounts ($8,000-$9,999) to the SAME receiver within a
24-hour window - a classic indicator of "smurfing" to stay below the
$10,000 BSA/CTR reporting limit.

Two stages:
  1. Filter near-threshold transfers & assign 24h day buckets
  2. Self-join + aggregate fan-in metrics per receiver per day

Requires:
  - money_transfers feature group (run transfers-datamart first)

Usage:
    python ccfraud/2e-fan-in-spark-pipeline.py
"""

import hopsworks
from pyspark.sql import functions as F

project = hopsworks.login()
fs = project.get_feature_store()

# ---------------------------------------------------------------------------
# Step 1: Read source feature group
# ---------------------------------------------------------------------------
transfers_fg = fs.get_feature_group('money_transfers', version=1)
transfers_df = transfers_fg.read(dataframe_type='spark')

print(f'Money transfers: {transfers_df.count()} rows')
transfers_df.printSchema()

# ---------------------------------------------------------------------------
# Step 2: Filter near-threshold & assign 24h day buckets
# ---------------------------------------------------------------------------
SECONDS_PER_DAY = 86400

near_threshold_df = (
    transfers_df
    .filter((F.col('amount') >= 8000.0) & (F.col('amount') < 10000.0))
    .withColumn('day_bucket', F.floor(F.unix_timestamp('event_time') / SECONDS_PER_DAY))
)

print(f'Near-threshold transfers: {near_threshold_df.count()} rows')

# ---------------------------------------------------------------------------
# Step 3: Self-join to isolate fan-in patterns
# A transfer is kept only when at least one OTHER near-threshold transfer
# from a DIFFERENT sender was sent to the SAME receiver in the SAME 24h window.
# ---------------------------------------------------------------------------
a = near_threshold_df.alias('a')
b = near_threshold_df.alias('b')

fan_in_join_df = (
    a.join(
        b,
        (F.col('a.receiver_id') == F.col('b.receiver_id')) &
        (F.col('a.day_bucket') == F.col('b.day_bucket')) &
        (F.col('a.sender_id') != F.col('b.sender_id')),
    )
    .select(
        F.col('a.receiver_id').alias('receiver_id'),
        F.col('a.day_bucket').alias('day_bucket'),
        F.col('a.sender_id').alias('sender_id'),
        F.col('a.amount').alias('amount'),
        F.col('a.event_time').alias('event_time'),
    )
    .distinct()
)

# ---------------------------------------------------------------------------
# Step 4: Aggregate fan-in metrics per receiver per 24h window
# ---------------------------------------------------------------------------
fan_in_df = (
    fan_in_join_df
    .groupBy('receiver_id', 'day_bucket')
    .agg(
        F.max('event_time').cast('timestamp').alias('event_time'),
        F.countDistinct('sender_id').alias('fan_in_sender_count'),
        F.count('*').alias('fan_in_transfer_count'),
        F.sum('amount').alias('fan_in_total_amount'),
        F.avg('amount').alias('fan_in_avg_amount'),
        F.min('amount').alias('fan_in_min_amount'),
        F.max('amount').alias('fan_in_max_amount'),
    )
)

print(f'Fan-in features: {fan_in_df.count()} rows')
fan_in_df.show(10)

# ---------------------------------------------------------------------------
# Step 5: Write to feature group
# ---------------------------------------------------------------------------
fan_in_fg = fs.get_or_create_feature_group(
    name='fan_in_features_fg',
    version=1,
    description=(
        'Fan-in detection features per receiver per 24-hour window. '
        'Counts distinct senders sending near-threshold amounts '
        '($8K-$10K) to the same receiver - potential structuring.'
    ),
    primary_key=['receiver_id'],
    event_time='event_time',
    online_enabled=True,
)

fan_in_fg.insert(fan_in_df, wait=True)
print('Fan-in Spark feature pipeline completed successfully!')
