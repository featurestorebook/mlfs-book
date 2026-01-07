from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct
from pyspark.sql.types import StructType, StructField, FloatType, StringType
import xgboost as xgb
import joblib
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("PySparkXGBoostStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema of incoming JSON data
schema = StructType([
    StructField("feature1", FloatType(), True),
    StructField("feature2", FloatType(), True),
    StructField("feature3", FloatType(), True)
])

# Load your trained XGBoost model
model_path = "/path/to/your/xgboost_model.joblib"
xgb_model = joblib.load(model_path)

# Broadcast the model to Spark workers
broadcast_model = spark.sparkContext.broadcast(xgb_model)

# Define prediction function using pandas_udf
@pandas_udf(DoubleType())
def predict_udf(feature1: pd.Series, feature2: pd.Series, feature3: pd.Series) -> pd.Series:
    features_df = pd.DataFrame({
        'feature1': feature1,
        'feature2': feature2,
        'feature3': feature3
    })
    model = broadcast_model.value
    predictions = model.predict(features_df)
    return pd.Series(predictions)

# Streaming input (example from Kafka)
raw_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "your-topic-name") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON messages from Kafka
json_stream = raw_stream.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Apply the model to make predictions
predictions_stream = json_stream.withColumn(
    "prediction",
    predict_udf(col("feature1"), col("feature2"), col("feature3"))
)

# Write predictions to the console (for simplicity)
query = predictions_stream.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()

query.awaitTermination()

