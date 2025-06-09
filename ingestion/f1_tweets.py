import csv
import time
import json
from datetime import datetime
from kafka import KafkaProducer
import logging
import random

class TweetProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers='localhost:29092',
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            acks='all',
            retries=3,
            linger_ms=10,
            batch_size=32768
        )
        self.topic = 'f1-tweets'
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    def to_int_safe(self,value):
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return 0
    def clean_tweet(self, tweet):
        """Convert CSV string values to proper data types"""
        return {
            'user': {
                'name': tweet['user_name'],
                'location': tweet['user_location'],
                'description': tweet['user_description'],
                'created': tweet['user_created'],
                'followers': self.to_int_safe(tweet['user_followers']),
                'friends': self.to_int_safe(tweet['user_friends']),
                'favourites': self.to_int_safe(tweet['user_favourites']),

                'verified': tweet['user_verified'].lower() == 'true'
            },
            'tweet': {
                'date': tweet['date'],
                'text': tweet['text'],
                'hashtags': [h.strip() for h in tweet['hashtags'].split(',')] if tweet['hashtags'] else [],
                'source': tweet['source'],
                'is_retweet': tweet['is_retweet'].lower() == 'true'
            },
            'metadata': {
                'processed_at': datetime.utcnow().isoformat()
            }
        }

    def simulate_stream(self, csv_path, speed_factor=1.0):
        """Stream tweets with realistic timing patterns"""
        try:
            with open(csv_path, mode='r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                tweets = [self.clean_tweet(row) for row in reader]
                
                self.logger.info(f"Loaded {len(tweets)} tweets. Beginning simulation...")
                
                base_delay = 1.0 / speed_factor
                for tweet in tweets:
                    try:
                        if 'date' in tweet['tweet'] and len(tweets) > 1:
                            idx = tweets.index(tweet) % len(tweets)
                            if idx > 0:
                                prev_time = datetime.strptime(tweets[idx-1]['tweet']['date'], '%Y-%m-%d %H:%M:%S')
                                curr_time = datetime.strptime(tweet['tweet']['date'], '%Y-%m-%d %H:%M:%S')
                                delay = (curr_time - prev_time).total_seconds() / speed_factor
                                delay = max(0.1, min(delay, 5.0))  # Constrain between 0.1-5 seconds
                                time.sleep(delay)
                        
                        self.producer.send(self.topic, tweet)
                        self.logger.debug(f"Sent tweet from {tweet['user']['name']}")
                        
                    except Exception as e:
                        self.logger.error(f"Error processing tweet: {e}")
                        time.sleep(base_delay)
                        
        except KeyboardInterrupt:
            self.logger.info("Stopping simulation...")
        finally:
            self.producer.flush()
            self.producer.close()
            self.logger.info("Simulation complete")

if __name__ == "__main__":
    producer = TweetProducer()
    
    # Configuration
    CSV_PATH = "data\F1_tweets.csv" 
    SPEED_FACTOR = 1.0  # realtime
    
    producer.simulate_stream(CSV_PATH, SPEED_FACTOR)