#!/usr/bin/env python3
"""
Merchant Activity Drop Detection - Spark Batch Feature Pipeline

Reads from 3 feature groups (merchant_activity, terminal_events,
support_tickets), computes rolling baselines, drop detection, and
enrichment features via multi-table joins, then writes results to
the merchant_drop_features_fg feature group.

Steps:
  1. Read all 3 source FGs
  2. Day-bucket all tables
  3. 30-day rolling baseline per merchant
  4. Drop detection (volume < 50% of baseline)
  5. Days since last active transaction
  6. Support tickets in 30-day window
  7. Terminal offline count in 7-day window
  8. Write to merchant_drop_features_fg

Requires:
  - merchant_activity, terminal_events, support_tickets feature groups
    (run merchant-datamart first)

Usage:
    python ccfraud/2f-merchant-drop-spark-pipeline.py
"""

import hopsworks
from pyspark.sql import functions as F
from pyspark.sql import Window

project = hopsworks.login()
fs = project.get_feature_store()

# ---------------------------------------------------------------------------
# Step 1: Read source feature groups
# ---------------------------------------------------------------------------
activity_fg = fs.get_feature_group('merchant_activity', version=1)
terminal_fg = fs.get_feature_group('terminal_events', version=1)
tickets_fg = fs.get_feature_group('support_tickets', version=1)

activity_df = activity_fg.read(dataframe_type='spark')
terminal_df = terminal_fg.read(dataframe_type='spark')
tickets_df = tickets_fg.read(dataframe_type='spark')

print(f'merchant_activity: {activity_df.count()} rows')
print(f'terminal_events:   {terminal_df.count()} rows')
print(f'support_tickets:   {tickets_df.count()} rows')

# ---------------------------------------------------------------------------
# Step 2: Day buckets on all tables
# ---------------------------------------------------------------------------
SECONDS_PER_DAY = 86400

activity_df = activity_df.withColumn(
    'day_bucket', F.floor(F.unix_timestamp('event_time') / SECONDS_PER_DAY)
)
terminal_df = terminal_df.withColumn(
    'day_bucket', F.floor(F.unix_timestamp('event_time') / SECONDS_PER_DAY)
)
tickets_df = tickets_df.withColumn(
    'day_bucket', F.floor(F.unix_timestamp('event_time') / SECONDS_PER_DAY)
)

# ---------------------------------------------------------------------------
# Step 3: 30-day rolling baseline per merchant (excludes current day)
# ---------------------------------------------------------------------------
w30 = (
    Window.partitionBy('merchant_id')
    .orderBy('day_bucket')
    .rowsBetween(-30, -1)
)

activity_df = (
    activity_df
    .withColumn('rolling_avg_volume_30d', F.avg('txn_volume').over(w30))
    .withColumn('rolling_avg_count_30d', F.avg('txn_count').over(w30))
)

# ---------------------------------------------------------------------------
# Step 4: Drop detection
# is_drop = True when txn_volume < rolling_avg_volume_30d * 0.5
#           and baseline is not null/zero
# ---------------------------------------------------------------------------
activity_df = activity_df.withColumn(
    'is_drop',
    F.when(
        (F.col('rolling_avg_volume_30d').isNotNull()) &
        (F.col('rolling_avg_volume_30d') > 0) &
        (F.col('txn_volume') < F.col('rolling_avg_volume_30d') * 0.5),
        F.lit(True),
    ).otherwise(F.lit(False)),
)

# ---------------------------------------------------------------------------
# Step 5: days_since_last_active_txn
# Mark active days (txn_count > 0) with their day_bucket, else null.
# Running max over unboundedPreceding..0 gives last active day.
# Subtract from current day_bucket.
# ---------------------------------------------------------------------------
w_running = (
    Window.partitionBy('merchant_id')
    .orderBy('day_bucket')
    .rowsBetween(Window.unboundedPreceding, 0)
)

activity_df = (
    activity_df
    .withColumn(
        'active_day',
        F.when(F.col('txn_count') > 0, F.col('day_bucket')),
    )
    .withColumn('last_active_day', F.max('active_day').over(w_running))
    .withColumn(
        'days_since_last_active_txn',
        F.col('day_bucket') - F.col('last_active_day'),
    )
    .drop('active_day', 'last_active_day')
)

# ---------------------------------------------------------------------------
# Step 6: support_tickets_30d (rolling 30-day ticket count)
# Pre-aggregate tickets per merchant per day, left join to activity,
# then rolling 30-day sum.
# ---------------------------------------------------------------------------
daily_tickets = (
    tickets_df
    .groupBy('merchant_id', 'day_bucket')
    .agg(F.count('*').alias('daily_ticket_count'))
)

activity_df = (
    activity_df
    .join(
        daily_tickets.withColumnRenamed('merchant_id', 't_merchant_id'),
        (F.col('merchant_id') == F.col('t_merchant_id')) &
        (activity_df['day_bucket'] == daily_tickets['day_bucket']),
        'left',
    )
    .drop('t_merchant_id')
    .drop(daily_tickets['day_bucket'])
    .withColumn(
        'daily_ticket_count',
        F.coalesce(F.col('daily_ticket_count'), F.lit(0)),
    )
)

w30_tickets = (
    Window.partitionBy('merchant_id')
    .orderBy('day_bucket')
    .rowsBetween(-30, -1)
)

activity_df = (
    activity_df
    .withColumn('support_tickets_30d', F.sum('daily_ticket_count').over(w30_tickets))
    .withColumn(
        'support_tickets_30d_before_drop',
        F.when(F.col('is_drop'), F.col('support_tickets_30d')),
    )
    .drop('daily_ticket_count')
)

# ---------------------------------------------------------------------------
# Step 7: terminal_offline_count_7d
# Filter terminal events to event_type == 'offline', aggregate per merchant
# per day, left join to activity, rolling 7-day sum.
# ---------------------------------------------------------------------------
offline_daily = (
    terminal_df
    .filter(F.col('event_type') == 'offline')
    .groupBy('merchant_id', 'day_bucket')
    .agg(F.count('*').alias('daily_offline_count'))
)

activity_df = (
    activity_df
    .join(
        offline_daily.withColumnRenamed('merchant_id', 'o_merchant_id'),
        (F.col('merchant_id') == F.col('o_merchant_id')) &
        (activity_df['day_bucket'] == offline_daily['day_bucket']),
        'left',
    )
    .drop('o_merchant_id')
    .drop(offline_daily['day_bucket'])
    .withColumn(
        'daily_offline_count',
        F.coalesce(F.col('daily_offline_count'), F.lit(0)),
    )
)

w7 = (
    Window.partitionBy('merchant_id')
    .orderBy('day_bucket')
    .rowsBetween(-7, 0)
)

activity_df = (
    activity_df
    .withColumn('terminal_offline_count_7d', F.sum('daily_offline_count').over(w7))
    .drop('daily_offline_count')
)

# ---------------------------------------------------------------------------
# Step 8: Select final columns and write to feature group
# ---------------------------------------------------------------------------
result_df = activity_df.select(
    'merchant_id',
    F.col('event_time').cast('timestamp').alias('event_time'),
    'txn_count',
    'txn_volume',
    'rolling_avg_volume_30d',
    'rolling_avg_count_30d',
    'is_drop',
    'days_since_last_active_txn',
    'support_tickets_30d',
    'support_tickets_30d_before_drop',
    'terminal_offline_count_7d',
)

print(f'\nMerchant drop features: {result_df.count()} rows')
result_df.show(10)

# Write to Hopsworks
merchant_drop_fg = fs.get_or_create_feature_group(
    name='merchant_drop_features_fg',
    version=1,
    description=(
        'Merchant activity drop detection features. Joins merchant_activity, '
        'terminal_events, and support_tickets to compute rolling baselines, '
        'drop flags, and enrichment signals.'
    ),
    primary_key=['merchant_id'],
    event_time='event_time',
    online_enabled=True,
)

merchant_drop_fg.insert(result_df, wait=True)
print('Merchant drop Spark feature pipeline completed successfully!')
