#!/usr/bin/env python3
"""
Routing Cost Feature Pipeline - Spark Batch

Reads money_transfers, bank_details, and card_details feature groups,
joins them, and computes per-transfer routing cost based on card type
and cross-border surcharge.

Steps:
  1. Read money_transfers, bank_details, and card_details FGs
  2. Join money_transfers with bank_details for sender country
  3. Join with bank_details again for receiver country
  4. Compute region_cross (sender and receiver in different countries)
  5. Join with card_details for card_type (most recent card per account)
  6. Compute estimated_cost (base cost by card type + cross-border surcharge)
  7. Write to routing_cost_fg

Requires:
  - money_transfers feature group
  - bank_details feature group
  - card_details feature group

Usage:
    python ccfraud/2h-routing-cost-spark-pipeline.py
"""

import hopsworks
from pyspark.sql import functions as F
from pyspark.sql import Window

project = hopsworks.login()
fs = project.get_feature_store()

# ---------------------------------------------------------------------------
# Step 1: Read source feature groups
# ---------------------------------------------------------------------------
transfers_fg = fs.get_feature_group('money_transfers', version=1)
bank_fg = fs.get_feature_group('bank_details', version=1)
card_fg = fs.get_feature_group('card_details', version=1)

transfers_df = transfers_fg.read(dataframe_type='spark')
bank_df = bank_fg.read(dataframe_type='spark')
card_df = card_fg.read(dataframe_type='spark')

print(f'money_transfers: {transfers_df.count()} rows')
print(f'bank_details:    {bank_df.count()} rows')
print(f'card_details:    {card_df.count()} rows')

# ---------------------------------------------------------------------------
# Step 2: Join money_transfers with bank_details (sender bank)
# ---------------------------------------------------------------------------
sender_bank = bank_df.select(
    F.col('bank_id').alias('s_bank_id'),
    F.col('country').alias('sender_country'),
)

transfers_df = transfers_df.join(
    sender_bank,
    transfers_df['sender_bank_id'] == sender_bank['s_bank_id'],
    'left',
).drop('s_bank_id')

# ---------------------------------------------------------------------------
# Step 3: Join with bank_details again (receiver bank)
# ---------------------------------------------------------------------------
receiver_bank = bank_df.select(
    F.col('bank_id').alias('r_bank_id'),
    F.col('country').alias('receiver_country'),
)

transfers_df = transfers_df.join(
    receiver_bank,
    transfers_df['receiver_bank_id'] == receiver_bank['r_bank_id'],
    'left',
).drop('r_bank_id')

# ---------------------------------------------------------------------------
# Step 4: Compute region_cross
# ---------------------------------------------------------------------------
transfers_df = transfers_df.withColumn(
    'region_cross',
    F.col('sender_country') != F.col('receiver_country'),
)

# ---------------------------------------------------------------------------
# Step 5: Join with card_details for card_type (most recent per account)
# ---------------------------------------------------------------------------
card_window = Window.partitionBy('account_id').orderBy(F.col('last_modified').desc())

latest_card = (
    card_df
    .withColumn('rn', F.row_number().over(card_window))
    .filter(F.col('rn') == 1)
    .select(
        F.col('account_id').alias('c_account_id'),
        'card_type',
    )
)

transfers_df = transfers_df.join(
    latest_card,
    transfers_df['sender_id'] == latest_card['c_account_id'],
    'left',
).drop('c_account_id')

# ---------------------------------------------------------------------------
# Step 6: Compute estimated_cost
# ---------------------------------------------------------------------------
base_cost = (
    F.when(F.col('card_type') == 'Debit', F.lit(0.50))
     .when(F.col('card_type') == 'Credit', F.lit(1.50))
     .when(F.col('card_type') == 'Prepaid', F.lit(1.00))
     .otherwise(F.lit(1.00))
)

surcharge = F.when(F.col('region_cross'), F.lit(1.00)).otherwise(F.lit(0.00))

transfers_df = transfers_df.withColumn(
    'estimated_cost', base_cost + surcharge,
)

# ---------------------------------------------------------------------------
# Step 7: Select final columns and write to feature group
# ---------------------------------------------------------------------------
result_df = transfers_df.select(
    F.col('transfer_id').alias('t_id'),
    'estimated_cost',
    'region_cross',
    'event_time',
)

print(f'\nRouting cost features: {result_df.count()} rows')
result_df.show(10)

# Write to Hopsworks
routing_fg = fs.get_or_create_feature_group(
    name='routing_cost_fg',
    version=1,
    description=(
        'Per-transfer routing cost features. Joins money_transfers with '
        'bank_details (sender/receiver) and card_details to derive '
        'region_cross and estimated_cost.'
    ),
    primary_key=['t_id'],
    event_time='event_time',
    online_enabled=True,
)

routing_fg.insert(result_df, wait=True)
print('Routing cost Spark feature pipeline completed successfully!')
