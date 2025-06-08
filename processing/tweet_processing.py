import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import re
import emoji
from langdetect import detect
from googletrans import Translator
import google.generativeai as genai
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

def main():
    # Initialize Spark with optimized configurations
    spark = SparkSession.builder \
        .appName("F1TweetSentimentAnalysis") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .config("spark.mongodb.output.uri", os.getenv("MONGOATLAS_URI")) \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()

    # Define complete schema matching your CSV
    schema = StructType([
        StructField("user_name", StringType()),
        StructField("user_location", StringType()),
        StructField("user_description", StringType()),
        StructField("user_created", TimestampType()),
        StructField("user_followers", StringType()),
        StructField("user_friends", StringType()),
        StructField("user_favourites", StringType()),
        StructField("user_verified", StringType()),
        StructField("date", TimestampType()),
        StructField("text", StringType()),
        StructField("hashtags", StringType()),
        StructField("source", StringType()),
        StructField("is_retweet", StringType())
    ])

    # 1. Kafka source with enhanced configuration
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers","kafka:9092") \
        .option("subscribe", "f1-tweets") \
        .option("maxOffsetsPerTrigger", 100) \
        .option("startingOffsets", "earliest") \
        .load()
        #.option("startingOffsets", "latest") \
        #.option("failOnDataLoss", "false") \
        #.load()
        

    test_query = kafka_df.select(
        col("key").cast("string"),
        col("value").cast("string"),
        col("topic"),
        col("partition"),
        col("offset")
    ).writeStream \
    .format("console") \
    .option("truncate", False) \
    .start()
    time.sleep(30)
    test_query.stop()

    # 2. Data transformation pipeline
    transformed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("processed_at", lit(datetime.utcnow().isoformat())) \
        .withColumn("cleaned_text", clean_text_udf(col("text"))) \
        .withColumn("language", detect_language_udf(col("cleaned_text"))) \
        .withColumn("english_text", 
                   when(col("language") != "en", translate_udf(col("cleaned_text")))
                   .otherwise(col("cleaned_text")))

    # 3. Initialize Gemini 1.5 Flash
    genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
    model = genai.GenerativeModel('gemini-1.5-flash')

    # 4. Sentiment analysis with enhanced prompt
    analyzed_df = transformed_df.withColumn(
        "sentiment", 
        analyze_sentiment_udf(col("english_text"))
    )

    # 5. Prepare final document structure
    final_df = analyzed_df.withColumn("mongo_document", struct(
        # User object
        struct(
            col("user_name").alias("name"),
            col("user_location").alias("location"),
            col("user_description").alias("description"),
            col("user_created").alias("created"),
            col("user_followers").cast("integer").alias("followers"),
            col("user_friends").cast("integer").alias("friends"),
            col("user_favourites").cast("integer").alias("favourites"),
            (lower(col("user_verified")) == "true").alias("verified")
        ).alias("user"),
        
        # Tweet object
        struct(
            col("date").alias("date"),
            col("text").alias("text"),
            split(col("hashtags"), ",").alias("hashtags"),
            col("source").alias("source"),
            (lower(col("is_retweet"))== "true").alias("is_retweet"),
            col("sentiment").alias("sentiment")
        ).alias("tweet"),
        
        # Metadata
        struct(
            col("processed_at").alias("processed_at"),
            col("language").alias("detected_language")
        ).alias("metadata")
    ))

    # 6. Write to MongoDB Atlas
    def write_to_mongo(batch_df, batch_id):
        (batch_df.select("mongo_document")
         .write
         .format("mongo")
         .mode("append")
         .option("collection", "f1_tweets_analyzed")
         .save())
        
        # Logging
        print(f"Processed batch {batch_id} with {batch_df.count()} records")

    query = final_df.writeStream \
        .foreachBatch(write_to_mongo) \
        .outputMode("append") \
        .option("checkpointLocation", os.getenv("CHECKPOINT_LOCATION")) \
        .start()

    query.awaitTermination()

# ===== UDF Definitions =====
def clean_text(text):
    """Enhanced text cleaning"""
    if not text:
        return ""
    
    # Remove URLs
    text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
    # Remove mentions
    text = re.sub(r'@\w+', '', text)
    # Remove emojis
    text = emoji.replace_emoji(text, replace="")
    # Remove special chars but keep basic punctuation
    text = re.sub(r"[^\w\s.!?,]", "", text)
    # Normalize whitespace
    text = " ".join(text.split())
    
    return text.strip()

def detect_language(text):
    try:
        return detect(text) if text else "unknown"
    except:
        return "unknown"

def translate_to_english(text):
    if not text or len(text) < 3:
        return text
    try:
        translator = Translator()
        return translator.translate(text, dest='en').text
    except:
        return text

def analyze_sentiment(text):
    if not text or len(text) < 3:
        return "no text"
    
    try:
        prompt = f"""Analyze this Formula 1 related text and classify the sentiment as exactly one of:
        - positive
        - neutral
        - negative
        
        Consider these guidelines:
        1. Positive = Enthusiasm, praise, excitement
        2. Negative = Criticism, disappointment, anger
        3. Neutral = Facts, questions, neutral statements
        
        Text: {text[:1000]}
        """
        response = model.generate_content(prompt)
        time.sleep(0.5)  # Rate limiting for Gemini 1.5 Flash
        result = response.text.lower().strip()
        return result if result in ["positive", "neutral", "negative"] else "no model response"
    except Exception as e:
        print(f"Sentiment analysis error: {str(e)}")
        return "error"

# Register UDFs
clean_text_udf = udf(clean_text, StringType())
detect_language_udf = udf(detect_language, StringType())
translate_udf = udf(translate_to_english, StringType())
analyze_sentiment_udf = udf(analyze_sentiment, StringType())

if __name__ == "__main__":
    main()