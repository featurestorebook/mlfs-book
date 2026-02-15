import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

import hopsworks
import hsfs

# Setup paths and config
root_dir = Path().absolute()
# Strip /notebooks or /ccfraud from path if notebook started in one of these subdirectories
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

# # Initialize Spark
# spark = SparkSession.builder \
#     .appName('MCC Spend Ratio Feature Pipeline') \
#     .getOrCreate()


project = hopsworks.login()
fs = project.get_feature_store()

trans_fg = fs.get_feature_group('credit_card_transactions', version=1)
merchant_fg = fs.get_feature_group('merchant_details', version=1)

trans_df = trans_fg.read(dataframe_type='spark')
merchant_df = merchant_fg.read(dataframe_type='spark')

print(f'Transactions: {trans_df.count()} rows')
print(f'Merchants: {merchant_df.count()} rows')

trans_with_cat = trans_df.join(
    merchant_df.select('merchant_id', 'category'),
    on='merchant_id',
    how='inner'
)
trans_with_cat.printSchema()

days_90_seconds = 90 * 24 * 60 * 60
window_spec = (
    Window.partitionBy('category')
    .orderBy(F.unix_timestamp('ts'))
    .rangeBetween(-days_90_seconds, 0)
)

trans_with_avg = trans_with_cat.withColumn(
    'avg_amount_mcc_90d', F.avg('amount').over(window_spec)
)

# Compute ratio (handle null/zero avg)
trans_with_ratio = trans_with_avg.withColumn(
    'mcc_spend_ratio_90d',
    F.when(
        (F.col('avg_amount_mcc_90d').isNull()) | (F.col('avg_amount_mcc_90d') == 0),
        None
    ).otherwise(
        F.col('amount') / F.col('avg_amount_mcc_90d')
    )
)

# Deduplicate on (cc_num, ts), keeping last
dedup_window = Window.partitionBy('cc_num', 'ts').orderBy(F.col('t_id').desc())
trans_deduped = (
    trans_with_ratio
    .withColumn('_row_num', F.row_number().over(dedup_window))
    .filter(F.col('_row_num') == 1)
    .drop('_row_num')
)

# Select final columns
result_df = trans_deduped.select('cc_num', 'merchant_id', 'ts', 'mcc_spend_ratio_90d')
print(f'Result rows: {result_df.count()}')
result_df.show(10)

# Create or get the feature group
mcc_fg = fs.get_or_create_feature_group(
    name='mcc_spend_ratio',
    version=1,
    description='Ratio of transaction amount to the average spend at the same Merchant Category (MCC) over the last 90 days',
    primary_key=['cc_num'],
    event_time='ts',
    online_enabled=False,
)

# Insert into feature group
mcc_fg.insert(result_df, wait=True)
print('MCC spend ratio feature pipeline completed successfully!')
