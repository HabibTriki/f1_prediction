import os
import csv
import fastf1
import asyncio
from kafka import KafkaProducer
from dotenv import load_dotenv
import logging
from datetime import datetime
from io import StringIO
import shutil
from fastf1.req import RateLimitExceededError
from concurrent.futures import ThreadPoolExecutor
import functools

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

class AsyncF1HistoricalProducer:
    def __init__(self, *, clear_cache=False, log_level=None, mirror=None):
        self.sleep_time = int(os.getenv('FASTF1_SLEEP', '10'))
        self.kafka_broker = os.getenv('KAFKA_BROKER', 'localhost:29092')
        self.topic = os.getenv('HISTORICAL_TOPIC', 'f1-historical-data')
        self.cache_dir = os.getenv("FASTF1_CACHE", "fastf1_cache")

        if clear_cache and os.path.isdir(self.cache_dir):
            shutil.rmtree(self.cache_dir)

        if log_level is not None:
            logging.getLogger("fastf1.core").setLevel(log_level)
            logging.getLogger("fastf1.api").setLevel(log_level)

        if mirror:
            try:
                fastf1.config.load_config()
                fastf1.config.set("fallback_livetiming_mirror", mirror)
            except Exception:
                logger.warning("Failed to set FastF1 mirror URL")

        self.producer = KafkaProducer(
            bootstrap_servers=[self.kafka_broker],
            value_serializer=lambda x: x.encode('utf-8'),
            acks='all',
            retries=3
        )
        
        fastf1.Cache.enable_cache(self.cache_dir)
        
        self.executor = ThreadPoolExecutor(max_workers=os.cpu_count())

    def _fetch_session_sync(self, year, gp_name, session_type):
        """Synchronous session fetch (runs in thread pool)"""
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
                import time
                time.sleep(wait_time)
            except Exception as e:
                logger.error(
                    f"Error fetching {session_type} data for {year} {gp_name}: {e}"
                )
                return None

    async def fetch_session_data(self, year, gp_name, session_type):
        """Async wrapper for session data fetching"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.executor, 
            self._fetch_session_sync, 
            year, gp_name, session_type
        )

    def session_to_csv(self, session):
        """Convert FastF1 session data to CSV format"""
        if not session:
            return None
            
        csv_buffer = StringIO()
        writer = csv.writer(csv_buffer)
        
        writer.writerow([
            'Driver', 'LapNumber', 'LapTimeSeconds', 'TrackStatus',
            'Position', 'Compound', 'TyreLife', 'FreshTyre'
        ])
        
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
    
    async def _process_session_async(self, year, gp, session_type):
        """Async worker to fetch and send a single session"""
        try:
            session = await self.fetch_session_data(year, gp, session_type)
            if session:
                csv_data = self.session_to_csv(session)
                if csv_data:
                    key = f"{year}_{gp}_{session_type}"
                    self.producer.send(
                        self.topic,
                        key=key.encode("utf-8"),
                        value=csv_data,
                    )
                    logger.info(f"Sent {key} ({len(csv_data)} bytes)")
        except Exception as e:
            logger.error(f"Failed processing {year} {gp} {session_type}: {e}")

    async def _get_event_schedule_async(self, year):
        """Async wrapper for getting event schedule"""
        loop = asyncio.get_event_loop()
        try:
            schedule = await loop.run_in_executor(
                self.executor, 
                fastf1.get_event_schedule, 
                year
            )
            return schedule["EventName"].tolist()
        except Exception as e:
            logger.error(f"Failed to load schedule for {year}: {e}")
            return []

    async def produce_historical_data_async(self, start_year=2018, end_year=2025, gp_names=None):
        """Fetch and send historical data asynchronously for all years/GPs in parallel"""
        
        if gp_names:
            all_tasks = [
                self._process_session_async(year, gp, session_type)
                for year in range(start_year, end_year + 1)
                for gp in gp_names
                for session_type in ["Q", "R"]
            ]
        else:
            logger.info(f"Fetching schedules for years {start_year}-{end_year}...")
            schedules = await asyncio.gather(*[
                self._get_event_schedule_async(year) 
                for year in range(start_year, end_year + 1)
            ], return_exceptions=True)
            
            all_tasks = [
                self._process_session_async(year, gp, session_type)
                for year, schedule in zip(range(start_year, end_year + 1), schedules)
                if not isinstance(schedule, Exception) and schedule
                for gp in schedule
                for session_type in ["Q", "R"]
            ]
        
        logger.info(f"Starting parallel execution of {len(all_tasks)} sessions...")
        
        completed = 0
        total = len(all_tasks)
        
        batch_size = 50  
        for i in range(0, len(all_tasks), batch_size):
            batch = all_tasks[i:i + batch_size]
            await asyncio.gather(*batch, return_exceptions=True)
            completed += len(batch)
            logger.info(f"Progress: {completed}/{total} sessions processed")
            
            if i + batch_size < len(all_tasks):
                await asyncio.sleep(1)

        self.producer.flush()
        self.producer.close()
        self.executor.shutdown(wait=True)
        logger.info("Historical data production complete")

    def __del__(self):
        """Cleanup resources"""
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=False)

async def main():
    import argparse

    parser = argparse.ArgumentParser(description="Fetch historical F1 data asynchronously")
    parser.add_argument("--start-year", type=int, default=2018,
                        help="First season to download")
    parser.add_argument("--end-year", type=int, default=2025,
                        help="Last season to download")
    parser.add_argument("--gp", nargs="*", default=None,
                        help="Optional list of grands prix to process")
    parser.add_argument("--quiet", action="store_true",
                        help="Suppress FastF1 warnings")
    parser.add_argument("--clear-cache", action="store_true",
                        help="Delete cache directory before running")
    parser.add_argument("--mirror", default=None,
                        help="Alternative telemetry mirror base URL")
    args = parser.parse_args()

    log_level = logging.ERROR if args.quiet else None
    producer = AsyncF1HistoricalProducer(
        clear_cache=args.clear_cache,
        log_level=log_level,
        mirror=args.mirror,
    )
    
    await producer.produce_historical_data_async(
        start_year=args.start_year,
        end_year=args.end_year,
        gp_names=args.gp
    )

if __name__ == "__main__":
    asyncio.run(main())