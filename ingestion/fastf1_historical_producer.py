import os
import csv
import fastf1
from kafka import KafkaProducer
from dotenv import load_dotenv
import logging
from io import StringIO

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class F1HistoricalProducer:
    def __init__(self):
        self.kafka_broker = os.getenv('KAFKA_BROKER', 'localhost:29092')
        self.topic = os.getenv('HISTORICAL_TOPIC', 'f1-historical-data')
        
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=[self.kafka_broker],
            value_serializer=lambda x: x.encode('utf-8'),
            acks='all',
            retries=3
        )
        
        # Configure FastF1 cache
        fastf1.Cache.enable_cache('fastf1_cache')  # Local cache to avoid re-fetching

    def fetch_session_data(self, year, gp_name, session_type):
        """Fetch session data (qualifying/race) using FastF1"""
        try:
            session = fastf1.get_session(year, gp_name, session_type)
            session.load()
            return session
        except Exception as e:
            logger.error(f"Error fetching {session_type} data for {year} {gp_name}: {e}")
            return None

    def session_to_csv(self, session):
        """Convert FastF1 session data to CSV format"""
        if not session:
            return None
            
        csv_buffer = StringIO()
        writer = csv.writer(csv_buffer)
        
        # Write header
        writer.writerow([
            'Driver', 'LapNumber', 'LapTime', 'TrackStatus', 
            'Position', 'Compound', 'TyreLife', 'FreshTyre'
        ])
        
        # Write lap data
        for _, lap in session.laps.iterrows():
            writer.writerow([
                lap['Driver'], lap['LapNumber'], lap['LapTime'], 
                lap['TrackStatus'], lap['Position'], lap['Compound'], 
                lap['TyreLife'], lap['FreshTyre']
            ])
        
        return csv_buffer.getvalue()

    def produce_historical_data(self, years, gp_names):
        """Fetch and send historical data for multiple years/GPs"""
        for year in years:
            for gp in gp_names:
                for session_type in ['Q', 'R']:  # Qualifying and Race
                    try:
                        session = self.fetch_session_data(year, gp, session_type)
                        if session:
                            csv_data = self.session_to_csv(session)
                            if csv_data:
                                # Send to Kafka with metadata in key
                                key = f"{year}_{gp}_{session_type}"
                                self.producer.send(
                                    self.topic, 
                                    key=key.encode('utf-8'),
                                    value=csv_data
                                )
                                logger.info(f"Sent {key} ({len(csv_data)} bytes)")
                    
                    except Exception as e:
                        logger.error(f"Failed processing {year} {gp} {session_type}: {e}")
        
        self.producer.flush()
        self.producer.close()
        logger.info("Historical data production complete")

if __name__ == "__main__":
    producer = F1HistoricalProducer()
    
    # Example: Fetch data for 2020-2023 seasons for selected GPs
    producer.produce_historical_data(
        years=[2020, 2021, 2022, 2023],
        gp_names=['Monaco', 'Silverstone', 'Monza', 'Spa']
    )