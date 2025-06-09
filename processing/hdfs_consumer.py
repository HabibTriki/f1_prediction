import os
import logging
from kafka import KafkaConsumer
from dotenv import load_dotenv
from pathlib import Path
from hdfs import InsecureClient

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HistoricalHDFSConsumer:
    """Consume historical data from Kafka and store in HDFS."""

    def __init__(self):
        self.kafka_broker = os.getenv("KAFKA_BROKER", "localhost:29092")
        self.topic = os.getenv("HISTORICAL_TOPIC", "f1-historical-data")
        self.hdfs_url = os.getenv("HDFS_URL", "http://namenode:9870")
        self.hdfs_root = os.getenv("HDFS_PATH", "/data")

        self.hdfs_client = InsecureClient(self.hdfs_url)

        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=[self.kafka_broker],
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="historical-hdfs-consumer",
        )

    def write_message(self, key: str, value: bytes):
        year, gp_name, session_type = key.split("_")
        hdfs_path = f"{self.hdfs_root}/{year}/{gp_name}/{session_type}.csv"
        with self.hdfs_client.write(hdfs_path, overwrite=True) as writer:
            writer.write(value)
        logger.info("Wrote %s", hdfs_path)

    def consume(self):
        for msg in self.consumer:
            try:
                key = msg.key.decode("utf-8")
                self.write_message(key, msg.value)
            except Exception as exc:
                logger.error("Failed to store message: %s", exc)

if __name__ == "__main__":
    consumer = HistoricalHDFSConsumer()
    logger.info("Starting HDFS consumer...")
    consumer.consume()