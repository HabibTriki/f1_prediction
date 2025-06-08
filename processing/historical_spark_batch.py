import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from dotenv import load_dotenv

load_dotenv()


def wait_for_postgres(jdbc_url, user, password, max_retries=30, retry_delay=10):
    """Wait for PostgreSQL to be ready"""
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.getOrCreate()
    
    for attempt in range(max_retries):
        try:
            print(f"Attempting to connect to PostgreSQL (attempt {attempt + 1}/{max_retries})")
            
            test_df = spark.sql("SELECT 1 as test")
            test_df.write.format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", "(SELECT 1) as test_table") \
                .option("user", user) \
                .option("password", password) \
                .option("driver", "org.postgresql.Driver") \
                .mode("overwrite") \
                .save()
            
            print("PostgreSQL connection successful!")
            return True
            
        except Exception as e:
            print(f"Connection attempt {attempt + 1} failed: {str(e)}")
            if attempt == max_retries - 1:
                print("Max retries reached. PostgreSQL is not available.")
                return False
            time.sleep(retry_delay)
    
    return False


def main():
    spark = (
        SparkSession.builder.appName("HistoricalBatchProcessing")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.7.1"
        )
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    jdbc_url = os.getenv("POSTGRES_URL", "jdbc:postgresql://localhost:5432/formula1")
    table = os.getenv("POSTGRES_TABLE", "historical_avg_laps")
    user = os.getenv("POSTGRES_USER") or os.getenv("POSTGRESQL_USERNAME") or "f1user"
    password = os.getenv("POSTGRES_PASSWORD") or os.getenv("POSTGRESQL_PASSWORD") or "f1password"
    
    print(f"Connecting to: {jdbc_url}")
    print(f"User: {user}")
    print(f"Table: {table}")
    
    if not wait_for_postgres(jdbc_url, user, password):
        print("Failed to connect to PostgreSQL. Exiting.")
        spark.stop()
        return

    hdfs_root = os.getenv("HDFS_PATH", "hdfs_storage")
    input_path = os.path.join(hdfs_root, "mapreduce_output")

    schema = StructType([
        StructField("driver", StringType()),
        StructField("avg_lap_time", FloatType()),
    ])

    print(f"Reading data from: {input_path}")
    df = spark.read.csv(input_path, sep="\t", schema=schema)
    
    print("Sample data:")
    df.show(5)
    print(f"Total records: {df.count()}")

    properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
        "stringtype": "unspecified"
    }

    try:
        print("Writing data to PostgreSQL...")
        (df.write.mode("overwrite")
            .jdbc(jdbc_url, table, properties=properties))
        print("Data successfully written to PostgreSQL!")
        
    except Exception as e:
        print(f"Error writing to PostgreSQL: {str(e)}")
        raise

    spark.stop()


if __name__ == "__main__":
    main()