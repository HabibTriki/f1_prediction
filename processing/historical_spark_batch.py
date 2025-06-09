import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from dotenv import load_dotenv

load_dotenv()


def wait_for_database(jdbc_url, user, password, driver, max_retries=30, retry_delay=10):
    """Wait for the JDBC database to be ready by attempting a test write."""
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.getOrCreate()
    
    for attempt in range(max_retries):
        try:
            print(
                f"Attempting JDBC connection (attempt {attempt + 1}/{max_retries})"
            )

            test_df = spark.sql("SELECT 1 as test")
            (
                test_df.write.format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", "(SELECT 1) as test_table")
                .option("user", user)
                .option("password", password)
                .option("driver", driver)
                .mode("overwrite")
                .save()
            )

            print("JDBC connection successful!")
            return True
            
        except Exception as e:
            print(f"Connection attempt {attempt + 1} failed: {str(e)}")
            if attempt == max_retries - 1:
                print("Max retries reached. Database is not available.")
                return False
            time.sleep(retry_delay)
    
    return False


def main():
    jars = os.getenv(
        "JDBC_JARS",
        "org.postgresql:postgresql:42.7.1,com.microsoft.sqlserver:mssql-jdbc:12.6.0.jre8",
    )

    spark = (
        SparkSession.builder.appName("HistoricalBatchProcessing")
        .config("spark.jars.packages", jars)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    jdbc_url = os.getenv(
        "JDBC_URL",
        os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/formula1"),
    )
    table = os.getenv("POSTGRES_TABLE", "historical_avg_laps")
    user = os.getenv("POSTGRES_USER") or os.getenv("POSTGRESQL_USERNAME") or "f1user"
    password = os.getenv("POSTGRES_PASSWORD") or os.getenv("POSTGRESQL_PASSWORD") or "f1password"

    driver = os.getenv("JDBC_DRIVER")
    if not driver:
        if "sqlserver" in jdbc_url:
            driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        else:
            driver = "org.postgresql.Driver"
    
    print(f"Connecting to: {jdbc_url}")
    print(f"User: {user}")
    print(f"Table: {table}")

    if not wait_for_database(jdbc_url, user, password, driver):
        print("Failed to connect to the database. Exiting.")
        spark.stop()
        return

    hdfs_root = os.getenv("HDFS_PATH", "/data")
    hdfs_host = os.getenv("HDFS_HOST", "namenode")
    hdfs_port = os.getenv("HDFS_PORT", "9000")
    input_path = f"hdfs://{hdfs_host}:{hdfs_port}{os.path.join(hdfs_root, 'mapreduce_output')}"

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
        "driver": driver,
        "stringtype": "unspecified",
    }

    try:
        print("Writing data to database via JDBC...")
        (
            df.write.mode("overwrite")
            .jdbc(jdbc_url, table, properties=properties)
        )
        print("Data successfully written!")
        
    except Exception as e:
        print(f"Error writing via JDBC: {str(e)}")
        raise

    spark.stop()


if __name__ == "__main__":
    main()