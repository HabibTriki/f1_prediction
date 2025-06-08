import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from dotenv import load_dotenv

load_dotenv()


def main():
    spark = (
        SparkSession.builder.appName("HistoricalBatchProcessing")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.7.1"
        )
        .getOrCreate()
    )

    hdfs_root = os.getenv("HDFS_PATH", "hdfs_storage")
    input_path = os.path.join(hdfs_root, "mapreduce_output")

    schema = StructType([
        StructField("driver", StringType()),
        StructField("avg_lap_time", FloatType()),
    ])

    df = spark.read.csv(input_path, sep="\t", schema=schema)

    jdbc_url = os.getenv("POSTGRES_URL", "jdbc:postgresql://localhost:5432/formula1")
    table = os.getenv("POSTGRES_TABLE", "historical_avg_laps")
    properties = {
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "driver": "org.postgresql.Driver",
    }

    (df.write.mode("overwrite")
        .jdbc(jdbc_url, table, properties=properties))

    spark.stop()


if __name__ == "__main__":
    main()