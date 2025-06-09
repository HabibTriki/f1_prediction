import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json # Explicitly import 'col' and 'from_json'
from pyspark.sql.types import *
import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
import joblib
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("F1PredictionEngine") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
            "com.microsoft.sqlserver:mssql-jdbc:10.2.1.jre8") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# Load model and encoders
model = joblib.load('f1_model.pkl')
label_encoders = {
    feature_name: LabelEncoder() for feature_name in ['race', 'circuit', 'driver', 'constructor'] # Renamed 'col' to 'feature_name'
}
for feature_name in label_encoders: # Renamed 'col' to 'feature_name'
    label_encoders[feature_name].classes_ = np.load(f'label_encoder_{feature_name}.npy', allow_pickle=True)

# Get complete connection string from environment
CONNECTION_STRING = os.getenv('AZURE_SQL_JDBC')
KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', '')

def fetch_historical_averages(driver: str, constructor: str):
    """Fetch historical averages using the complete connection string"""
    query = f"""
    (SELECT
        driver_avg as avgDriverFinish,
        constructor_avg as avgConstructorFinish
      FROM historical_averages
      WHERE driver_name = '{driver}' AND constructor_name = '{constructor}'
    ) AS tmp
    """
    return spark.read \
        .format("jdbc") \
        .option("url", CONNECTION_STRING) \
        .option("dbtable", query) \
        .load()
def safe_encode(encoder, values):
    """Handle unknown categories during encoding"""
    try:
        return encoder.transform(values)
    except ValueError:
        # For unknown categories, return -1
        return np.array([-1 if x not in encoder.classes_ else encoder.transform([x])[0] 
                        for x in values])

def process_batch(batch_df, batch_id):
    """Process each micro-batch of data with mock averages for testing"""
    if batch_df.isEmpty():
        return
    
    # Convert to pandas for feature processing
    pdf = batch_df.toPandas()
    
    # Apply label encoding to categorical features
    for feature_col_name in label_encoders: # Renamed 'col' to 'feature_col_name'
        pdf[feature_col_name] = safe_encode(label_encoders[feature_col_name], pdf[feature_col_name].astype(str))
    
    
    # Mock data configuration
    USE_MOCK_AVERAGES = True  # Set to False to use real database values
    MOCK_DRIVER_AVG = 12.5    # Default mock driver average
    MOCK_CONSTRUCTOR_AVG = 8.2 # Default mock constructor average
    
    if USE_MOCK_AVERAGES:
        # Use mock averages for all records
        pdf['avgDriverFinish'] = MOCK_DRIVER_AVG
        pdf['avgConstructorFinish'] = MOCK_CONSTRUCTOR_AVG
        print("Using mock averages for testing")
    else:
        # Get unique driver-constructor pairs for this batch
        unique_pairs = pdf[['driver', 'constructor']].drop_duplicates()
        
        # Fetch historical averages for all unique pairs
        averages_map = {}
        for _, row in unique_pairs.iterrows():
            avg_data = fetch_historical_averages(row['driver'], row['constructor'])
            if avg_data.count() > 0:
                avg_row = avg_data.first()
                averages_map[(row['driver'], row['constructor'])] = (
                    avg_row['avgDriverFinish'],
                    avg_row['avgConstructorFinish']
                )
        
        # Apply averages to all records (with defaults if not found)
        pdf['avgDriverFinish'] = pdf.apply(
            lambda x: averages_map.get((x['driver'], x['constructor']), (15.0, 10.0))[0], 
            axis=1
        )
        pdf['avgConstructorFinish'] = pdf.apply(
            lambda x: averages_map.get((x['driver'], x['constructor']), (15.0, 10.0))[1], 
            axis=1
        )
    
    # Prepare features for prediction
    features = pdf[[
        'race', 'circuit', 'latitude', 'longitude', 'altitude',
        'driver', 'carNumber', 'constructor', 'avgDriverFinish',
        'avgConstructorFinish', 'lapNumber', 'lapPosition',
        'pitStop', 'pitCount', 'pitTime_ms'
    ]]
    
    # Make predictions
    predictions = model.predict(features)
    pdf['predicted_lap_time_ms'] = predictions
    
    # Print sample predictions for verification
    sample_size = min(3, len(pdf))
    print(f"Sample predictions (showing {sample_size} records):")
    print(pdf[['driver', 'constructor', 'avgDriverFinish', 'avgConstructorFinish', 'predicted_lap_time_ms']].head(sample_size))
    
    # Save predictions to database
    save_predictions(pdf)

def save_predictions(predictions_df):
    """Save predictions to database using the connection string"""
    # Select only the columns we want to store
    results = predictions_df[[
        'id', 'race', 'date', 'circuit', 'driver',
        'constructor', 'lapNumber', 'lapTime_ms',
        'predicted_lap_time_ms'
    ]]
    
    # Write to database
    spark.createDataFrame(results).write \
        .format("jdbc") \
        .option("url", CONNECTION_STRING) \
        .option("dbtable", "lap_time_predictions") \
        .option("user", os.getenv('DB_USER')) \
        .option("password", os.getenv('DB_PASSWORD')) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .mode("append") \
        .save()

# Define schema for incoming Kafka data
kafka_schema = StructType([ # Renamed schema to kafka_schema (good!)
    StructField("id", StringType()),
    StructField("race", StringType()),
    StructField("date", StringType()),
    StructField("circuit", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("altitude", DoubleType()),
    StructField("driver", StringType()),
    StructField("carNumber", IntegerType()),
    StructField("constructor", StringType()),
    StructField("lapNumber", IntegerType()),
    StructField("lapPosition", IntegerType()),
    StructField("pitStop", IntegerType()),
    StructField("pitCount", IntegerType()),
    StructField("pitTime_ms", IntegerType()),
    StructField("lapTime_ms", IntegerType()),
    StructField("timestamp", StringType())
])

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
    .option("subscribe", "prediction_lap") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", "1000") \
    .load()

# Parse JSON and select fields
parsed_df = kafka_df.select(
    from_json(col("value").cast(StringType()), kafka_schema).alias("data") # 'col' here refers to pyspark.sql.functions.col
).select("data.*")

# Start streaming processing
query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/checkpoints/f1_predictions") \
    .start()

query.awaitTermination()