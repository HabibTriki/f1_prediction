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
import time
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

class AsyncF1HistoricalProducer:
    def __init__(self, *, clear_cache=False, log_level=None, mirror=None):
        self.sleep_time = int(os.getenv('FASTF1_SLEEP', '30')) 
        self.kafka_broker = os.getenv('KAFKA_BROKER', 'localhost:29092')
        self.topic = os.getenv('HISTORICAL_TOPIC', 'f1-historical-data')
        self.cache_dir = os.getenv("FASTF1_CACHE", "fastf1_cache")
        
        self.api_calls_made = 0
        self.calls_per_hour = 400 
        self.hour_start_time = time.time()
        self.min_delay_between_calls = 12 
        self.last_api_call_time = 0

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
        
        self.executor = ThreadPoolExecutor(max_workers=min(4, os.cpu_count()))

    def _check_rate_limit(self):
        """Check and enforce rate limiting before making API calls"""
        current_time = time.time()
        
        if current_time - self.hour_start_time >= 3600:
            self.api_calls_made = 0
            self.hour_start_time = current_time
            logger.info("Rate limit counter reset - new hour started")
        
        if self.api_calls_made >= self.calls_per_hour:
            sleep_time = 3600 - (current_time - self.hour_start_time) + 60  
            logger.warning(f"Rate limit reached ({self.api_calls_made}/{self.calls_per_hour}). Sleeping for {sleep_time:.0f} seconds...")
            time.sleep(sleep_time)
            self.api_calls_made = 0
            self.hour_start_time = time.time()
        
        time_since_last_call = current_time - self.last_api_call_time
        if time_since_last_call < self.min_delay_between_calls:
            sleep_time = self.min_delay_between_calls - time_since_last_call
            time.sleep(sleep_time)
        
        self.api_calls_made += 1
        self.last_api_call_time = time.time()
        
        if self.api_calls_made % 10 == 0:
            logger.info(f"API calls made this hour: {self.api_calls_made}/{self.calls_per_hour}")

    def _fetch_session_sync(self, year, gp_name, session_type):
        """Synchronous session fetch (runs in thread pool)"""
        
        if 'pre-season' in gp_name.lower() or 'test' in gp_name.lower():
            logger.info(f"Skipping pre-season/test event: {gp_name}")
            return None
        
        max_retries = 5
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self._check_rate_limit()
                
                session = fastf1.get_session(year, gp_name, session_type)
                
                if not hasattr(session, 'date') or session.date is None:
                    logger.warning(f"No data available for {session_type} session at {year} {gp_name}")
                    return None
                
                try:
                    if hasattr(session.date, 'date'):
                        session_date = session.date.date()  
                    else:
                        session_date = session.date
                    
                    if session_date > datetime.now().date():
                        logger.info(f"Skipping future session: {year} {gp_name} {session_type}")
                        return None
                except (AttributeError, TypeError):
                    logger.warning(f"Could not compare date for {year} {gp_name} {session_type}, proceeding anyway")
                
                self._check_rate_limit()
                session.load()
                
                logger.info(f"Successfully loaded {year} {gp_name} {session_type}")
                return session
                
            except RateLimitExceededError as e:
                retry_count += 1
                base_wait = self.sleep_time * (2 ** retry_count)
                jitter = random.uniform(0.5, 1.5)
                wait_time = base_wait * jitter
                
                logger.warning(
                    f"Rate limit exceeded (attempt {retry_count}/{max_retries}) for {session_type} "
                    f"at {year} {gp_name}. Waiting {wait_time:.1f} seconds..."
                )
                time.sleep(wait_time)
                
            except Exception as e:
                error_msg = str(e).lower()
                if "no data available" in error_msg or "schedule data" in error_msg:
                    logger.info(f"No data available for {year} {gp_name} {session_type}")
                    return None
                else:
                    retry_count += 1
                    if retry_count < max_retries:
                        wait_time = 5 * retry_count
                        logger.warning(f"Error fetching {session_type} for {year} {gp_name} (attempt {retry_count}): {e}. Retrying in {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"Failed to fetch {session_type} for {year} {gp_name} after {max_retries} attempts: {e}")
                        return None
        
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
        if not session or not hasattr(session, 'laps') or session.laps.empty:
            return None
            
        try:
            csv_buffer = StringIO()
            writer = csv.writer(csv_buffer)
            
            writer.writerow([
                'Driver', 'LapNumber', 'LapTimeSeconds', 'TrackStatus',
                'Position', 'Compound', 'TyreLife', 'FreshTyre'
            ])
            
            for _, lap in session.laps.iterrows():
                lap_time_seconds = None
                if hasattr(lap['LapTime'], 'total_seconds'):
                    lap_time_seconds = lap['LapTime'].total_seconds()
                elif lap['LapTime'] is not None:
                    lap_time_seconds = float(lap['LapTime'])
                
                writer.writerow([
                    lap.get('Driver', ''),
                    lap.get('LapNumber', ''),
                    lap_time_seconds,
                    lap.get('TrackStatus', ''),
                    lap.get('Position', ''),
                    lap.get('Compound', ''),
                    lap.get('TyreLife', ''),
                    lap.get('FreshTyre', ''),
                ])
            
            return csv_buffer.getvalue()
        except Exception as e:
            logger.error(f"Error converting session to CSV: {e}")
            return None
    
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
                    return True
        except Exception as e:
            logger.error(f"Failed processing {year} {gp} {session_type}: {e}")
        return False

    async def _get_event_schedule_async(self, year):
        """Async wrapper for getting event schedule"""
        loop = asyncio.get_event_loop()
        try:
            self._check_rate_limit()
            schedule = await loop.run_in_executor(
                self.executor, 
                fastf1.get_event_schedule, 
                year
            )
            
            current_date = datetime.now().date()
            past_events = []
            
            for _, event in schedule.iterrows():
                event_name = event['EventName']
                
                if 'pre-season' in event_name.lower() or 'test' in event_name.lower():
                    logger.info(f"Skipping pre-season/test event: {event_name}")
                    continue
                
                try:
                    event_date = None
                    for date_field in ['EventDate', 'Session5Date', 'Session4Date', 'Session3Date']:
                        if date_field in event and event[date_field] is not None:
                            if hasattr(event[date_field], 'date'):
                                event_date = event[date_field].date()
                            else:
                                event_date = event[date_field]
                            break
                    
                    if event_date and event_date <= current_date:
                        past_events.append(event_name)
                    elif event_date:
                        logger.info(f"Skipping future event: {event_name} ({event_date})")
                    else:
                        past_events.append(event_name)
                        
                except Exception as e:
                    logger.warning(f"Error processing event {event_name}: {e}")
                    # Include it anyway to be safe
                    past_events.append(event_name)
            
            logger.info(f"Found {len(past_events)} past events for {year}")
            return past_events
            
        except Exception as e:
            logger.error(f"Failed to load schedule for {year}: {e}")
            return []

    async def produce_historical_data_async(self, start_year=2018, end_year=2025, gp_names=None):
        """Fetch and send historical data year by year to avoid rate limiting issues"""
        
        total_successful = 0
        
        for year in range(start_year, end_year + 1):
            logger.info(f"\n{'='*50}")
            logger.info(f"Starting processing for year {year}")
            logger.info(f"{'='*50}")
            
            if gp_names:
                year_tasks = [
                    (year, gp, session_type)
                    for gp in gp_names
                    for session_type in ["Q", "R"]
                ]
            else:
                logger.info(f"Fetching schedule for {year}...")
                try:
                    schedule = await self._get_event_schedule_async(year)
                    if not schedule:
                        logger.warning(f"No events found for {year}, skipping...")
                        continue
                    
                    year_tasks = [
                        (year, gp, session_type)
                        for gp in schedule
                        for session_type in ["Q", "R"]
                    ]
                    logger.info(f"Found {len(schedule)} events for {year} ({len(year_tasks)} sessions total)")
                except Exception as e:
                    logger.error(f"Failed to get schedule for {year}: {e}")
                    continue
            
            year_successful = 0
            batch_size = 6  
            
            for i in range(0, len(year_tasks), batch_size):
                batch = year_tasks[i:i + batch_size]
                batch_num = i//batch_size + 1
                total_batches = (len(year_tasks) + batch_size - 1)//batch_size
                
                logger.info(f"Processing {year} batch {batch_num}/{total_batches} ({len(batch)} sessions)")
                
                batch_tasks = [
                    self._process_session_async(year, gp, session_type)
                    for year, gp, session_type in batch
                ]
                
                results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                batch_successful = sum(1 for r in results if r is True)
                year_successful += batch_successful
                
                logger.info(f"Batch {batch_num} complete: {batch_successful}/{len(batch)} successful")
                
                if i + batch_size < len(year_tasks):
                    logger.info("Waiting 10 seconds before next batch...")
                    await asyncio.sleep(10)
            
            total_successful += year_successful
            logger.info(f"Year {year} complete: {year_successful}/{len(year_tasks)} sessions successful")
            
            if year < end_year:
                logger.info("Waiting 15 seconds before next year...")
                await asyncio.sleep(15)
        
        self.producer.flush()
        self.producer.close()
        self.executor.shutdown(wait=True)
        logger.info(f"\n{'='*50}")
        logger.info(f"Historical data production complete!")
        logger.info(f"Total successful sessions: {total_successful}")
        logger.info(f"{'='*50}")

    def __del__(self):
        """Cleanup resources"""
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=False)

async def main():
    import argparse

    parser = argparse.ArgumentParser(description="Fetch historical F1 data asynchronously")
    parser.add_argument("--start-year", type=int, default=2018,
                        help="First season to download")
    parser.add_argument("--end-year", type=int, default=2024, 
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