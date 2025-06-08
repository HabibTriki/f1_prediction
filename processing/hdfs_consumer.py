import os
import logging
from kafka import KafkaConsumer
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HistoricalHDFSConsumer:
    """Consume historical data from Kafka and store in HDFS (local path)."""

    def __init__(self):
        self.kafka_broker = os.getenv("KAFKA_BROKER", "localhost:29092")
        self.topic = os.getenv("HISTORICAL_TOPIC", "f1-historical-data")
        # Local directory used to mimic HDFS
        self.hdfs_root = Path(os.getenv("HDFS_PATH", "hdfs_storage"))
        self.hdfs_root.mkdir(parents=True, exist_ok=True)

        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=[self.kafka_broker],
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="historical-hdfs-consumer",
        )

    def write_message(self, key: str, value: bytes):
        year, gp_name, session_type = key.split("_")
        dir_path = self.hdfs_root / year / gp_name
        dir_path.mkdir(parents=True, exist_ok=True)
        file_path = dir_path / f"{session_type}.csv"
        with open(file_path, "wb") as f:
            f.write(value)
        logger.info("Wrote %s", file_path)

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