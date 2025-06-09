import os
import csv
import fastf1
import time
from kafka import KafkaProducer
from dotenv import load_dotenv
import logging
from datetime import datetime
from io import StringIO
from fastf1.req import RateLimitExceededError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class F1HistoricalProducer:
    def __init__(self):
        self.kafka_broker = os.getenv('KAFKA_BROKER', 'localhost:29092')
        self.topic = os.getenv('HISTORICAL_TOPIC', 'f1-historical-data')
        self.sleep_time = int(os.getenv('FASTF1_SLEEP', '10'))

        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=[self.kafka_broker],
            value_serializer=lambda x: x.encode('utf-8'),
            acks='all',
            retries=3
        )
        
        # Configure FastF1 cache
        fastf1.Cache.enable_cache(os.getenv("FASTF1_CACHE", "fastf1_cache"))

    def fetch_session_data(self, year, gp_name, session_type):
        """Fetch session data (qualifying/race) using FastF1"""
        while True:
            try:
                session = fastf1.get_session(year, gp_name, session_type)
                session.load()
                return session
            except RateLimitExceededError as e:
                wait_time = self.sleep_time
                logger.warning(
                    f"Rate limit reached fetching {session_type} for {year} {gp_name}. "
                    f"Sleeping {wait_time} seconds..."
                )
                time.sleep(wait_time)
            except Exception as e:
                logger.error(
                    f"Error fetching {session_type} data for {year} {gp_name}: {e}"
                )
                return None

    def session_to_csv(self, session):
        """Convert FastF1 session data to CSV format"""
        if not session:
            return None
            
        csv_buffer = StringIO()
        writer = csv.writer(csv_buffer)
        
        # Write header
        writer.writerow([
            'Driver', 'LapNumber', 'LapTimeSeconds', 'TrackStatus',
            'Position', 'Compound', 'TyreLife', 'FreshTyre'
        ])
        
        # Write lap data
        for _, lap in session.laps.iterrows():
            writer.writerow([
                lap['Driver'],
                lap['LapNumber'],
                lap['LapTime'].total_seconds(),
                lap['TrackStatus'],
                lap['Position'],
                lap['Compound'],
                lap['TyreLife'],
                lap['FreshTyre'],
            ])
        
        return csv_buffer.getvalue()

    def produce_historical_data(self, years, gp_names):
        """Fetch and send historical data for multiple years/GPs"""
        for year in years:
            if not gp_names:
                try:
                    schedule = fastf1.get_event_schedule(year)
                    year_gps = schedule['EventName'].tolist()
                except Exception as e:
                    logger.error(f"Failed to load schedule for {year}: {e}")
                    continue
            else:
                year_gps = gp_names

            for gp in year_gps:
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
                                time.sleep(self.sleep_time)

                    except Exception as e:
                        logger.error(f"Failed processing {year} {gp} {session_type}: {e}")
        
        self.producer.flush()
        self.producer.close()
        logger.info("Historical data production complete")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch historical F1 data")
    parser.add_argument("start_year", type=int, nargs="?", default=2018,
                        help="First season to download")
    parser.add_argument("end_year", type=int, nargs="?",
                        default=datetime.utcnow().year,
                        help="Last season to download")
    parser.add_argument("--gp", nargs="*", default=[],
                        help="Optional list of grands prix to process")
    args = parser.parse_args()

    producer = F1HistoricalProducer()
    years = list(range(args.start_year, args.end_year + 1))
    producer.produce_historical_data(years=years, gp_names=args.gp)