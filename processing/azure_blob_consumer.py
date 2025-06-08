import os
from kafka import KafkaConsumer
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

class AzureBlobConsumer:
    def __init__(self):
        self.kafka_broker = os.getenv('KAFKA_BROKER', 'localhost:29092')
        self.topic = os.getenv('HISTORICAL_TOPIC', 'f1-historical-data')
        self.blob_conn_str = os.getenv('AZURE_BLOB_CONN_STR')
        self.container_name = "f1-raw"
        
        # Initialize Azure Blob client
        self.blob_service_client = BlobServiceClient.from_connection_string(self.blob_conn_str)
        self.container_client = self.blob_service_client.get_container_client(self.container_name)
        
        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=[self.kafka_broker],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='f1-historical-group'
        )

    def process_messages(self):
        for message in self.consumer:
            try:
                # Extract metadata from Kafka key
                key_parts = message.key.decode('utf-8').split('_')
                year, gp_name, session_type = key_parts
                
                # Create blob path
                blob_path = f"historical-f1/{year}/{gp_name}/{session_type}.csv"
                
                # Upload to Azure Blob
                blob_client = self.container_client.get_blob_client(blob_path)
                blob_client.upload_blob(message.value, overwrite=True)
                
                logging.info(f"Uploaded {blob_path} ({len(message.value)} bytes)")
                
            except Exception as e:
                logging.error(f"Failed to process message: {e}")

if __name__ == "__main__":
    consumer = AzureBlobConsumer()
    logging.info("Starting historical data consumer...")
    consumer.process_messages()