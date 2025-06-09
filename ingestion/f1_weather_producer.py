import os
import json
import time
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class F1WeatherProducer:
    def __init__(self):
        self.api_key = os.getenv('OPENWEATHER_API_KEY')
        self.kafka_broker = os.getenv('KAFKA_BROKER')
        self.topic = os.getenv('WEATHER_TOPIC')
        self.geocoding_key = os.getenv('GEOCODING_API_KEY', None)
        
        # F1 Circuit locations (lat, lon) - Can be expanded
        self.circuits = {
            'monaco': (43.7347, 7.4206),       # Circuit de Monaco
            'silverstone': (52.0786, -1.0169), # British GP
            'monza': (45.6156, 9.2814),        # Italian GP
            'spa': (50.4372, 5.9750),          # Belgian GP
            'interlagos': (-23.7036, -46.6997),# Brazilian GP
            'marina_bay': (1.2914, 103.8639),  # Singapore GP
            'current': None                     # For custom location
        }
        
        self.producer = KafkaProducer(
            bootstrap_servers=[self.kafka_broker],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            acks='all',
            retries=3
        )
    
    def get_circuit_coordinates(self, circuit_name):
        """Get coordinates for F1 circuits or geocode a location"""
        if circuit_name == 'current':
            # Get current race location (you could integrate with F1 schedule API)
            return self.fetch_current_race_location()
        elif circuit_name in self.circuits:
            return self.circuits[circuit_name]
        else:
            return self.geocode_location(circuit_name)
    
    def geocode_location(self, location_name):
        """Geocode a location name to coordinates"""
        if not self.geocoding_key:
            raise ValueError("Geocoding API key not configured")
        
        url = f"http://api.openweathermap.org/geo/1.0/direct?q={location_name}&limit=1&appid={self.api_key}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            if data:
                return (data[0]['lat'], data[0]['lon'])
            raise ValueError("Location not found")
        except Exception as e:
            logger.error(f"Geocoding error: {e}")
            raise
    
    def fetch_current_race_location(self):
        """In a real implementation, integrate with F1 schedule API"""
        # Placeholder - would normally fetch from F1 API
        return self.circuits['silverstone']  # Default to Silverstone
    
    def fetch_weather_data(self, lat, lon):
        """Fetch all weather data types for given coordinates"""
        base_url = "https://api.openweathermap.org/data/2.5"
        
        payload = {
            'current': f"{base_url}/weather?lat={lat}&lon={lon}&appid={self.api_key}",
            'forecast': f"{base_url}/forecast?lat={lat}&lon={lon}&appid={self.api_key}",
            'historical': f"{base_url}/onecall/timemachine?lat={lat}&lon={lon}&dt={int(time.time()-86400)}&appid={self.api_key}"
        }
        
        results = {}
        for data_type, url in payload.items():
            try:
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()
                data['data_type'] = data_type
                data['circuit_coords'] = {'lat': lat, 'lon': lon}
                results[data_type] = data
            except Exception as e:
                logger.error(f"Error fetching {data_type} weather: {e}")
        
        return results
    
    def produce_weather_data(self, circuit_name):
        """Main production loop for specific circuit"""
        lat, lon = self.get_circuit_coordinates(circuit_name)
        logger.info(f"Monitoring weather for {circuit_name} at ({lat}, {lon})")
        
        while True:
            try:
                weather_data = self.fetch_weather_data(lat, lon)
                
                for data_type, data in weather_data.items():
                    if data:
                        # For forecast, send each period separately
                        if data_type == 'forecast' and 'list' in data:
                            for period in data['list']:
                                period['data_type'] = 'forecast_period'
                                period['circuit_coords'] = data['circuit_coords']
                                self.producer.send(self.topic, value=period)
                        else:
                            self.producer.send(self.topic, value=data)
                
                logger.info(f"Weather data sent for {circuit_name} at {datetime.utcnow().isoformat()}")
                self.producer.flush()
                time.sleep(1800)  # 30 minute interval (respect API limits)
                
            except KeyboardInterrupt:
                logger.info("Shutting down producer...")
                self.producer.close()
                break
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                time.sleep(60)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('circuit', help="F1 circuit name or 'current'", 
                       choices=['monaco', 'silverstone', 'monza', 'spa', 'interlagos', 'marina_bay', 'current'])
    args = parser.parse_args()
    
    producer = F1WeatherProducer()
    producer.produce_weather_data(args.circuit)