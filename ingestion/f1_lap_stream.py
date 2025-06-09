import requests
import json
import time
import logging
from datetime import datetime
from kafka import KafkaProducer
import os

class F1LapStream:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers='localhost:29092',
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        )
        self.topic = 'prediction_lap'
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
        
        # Load circuit location data from JSON file
        self.locations = self._load_locations()
        self.pit_data_cache = {}  # Cache for pit times
    def get_pit_data(self, session_key, driver_number=None):
        """Fetch pit stop data for a session"""
        cache_key = f"{session_key}_{driver_number}" if driver_number else session_key
        if cache_key not in self.pit_data_cache:
            params = {'session_key': session_key}
            if driver_number:
                params['driver_number'] = driver_number
            try:
                response = requests.get("https://api.openf1.org/v1/pit", params=params)
                self.pit_data_cache[cache_key] = response.json() if response.status_code == 200 else []
            except Exception as e:
                self.logger.error(f"Failed to fetch pit data: {e}")
                self.pit_data_cache[cache_key] = []
        return self.pit_data_cache[cache_key]

    def _load_locations(self):
        """Load circuit locations from JSON file"""
        try:
            file_path = "C:/Users/neire/OneDrive/Documents/GL4/f1_prediction/data/f1-locations.json"
            with open(file_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to load locations file: {e}")
            return {}
    
    def _get_circuit_location(self, circuit_name):
        """Get location data from JSON file by circuit name or location"""
        if not circuit_name or not isinstance(circuit_name, str):
            return None
            
        circuit_name = circuit_name.strip().lower()
        
        for circuit in self.locations:
            # Get possible names from JSON entry
            json_names = []
            if circuit.get('circuit_short_name'):
                json_names.append(circuit['circuit_short_name'].strip().lower())
            if circuit.get('location'):
                json_names.append(circuit['location'].strip().lower())
            if circuit.get('circuit_name'):
                json_names.append(circuit['circuit_name'].strip().lower())
            
            # Check if our input name matches any of the JSON names
            if circuit_name in json_names:
                return {
                    'latitude': circuit.get('lat'),
                    'longitude': circuit.get('lon'),
                    'altitude': circuit.get('alt')
                }
        
        self.logger.warning(f"No location data found for circuit: {circuit_name}")
        return None

    def get_driver_info(self, driver_number):
        url = f"https://api.openf1.org/v1/drivers?driver_number={driver_number}"
        try:
            response = requests.get(url)
            return response.json()[0] if response.status_code == 200 and response.json() else None
        except Exception as e: 
            self.logger.warning(f"Driver fetch failed for #{driver_number}: {e}")
            return None

    def get_circuit_info(self, meeting_key):
        url = f"https://api.openf1.org/v1/meetings?meeting_key={meeting_key}"
        try:
            response = requests.get(url)
            if response.status_code == 200 and response.json():
                circuit_info = response.json()[0]
                
                # Check if we have location data in the API response
                if not all(key in circuit_info for key in ['circuit_latitude', 'circuit_longitude', 'circuit_altitude']):
                    # Fall back to our JSON file
                    location_data = self._get_circuit_location(circuit_info.get('circuit_short_name'))
                    if location_data:
                        circuit_info.update({
                            'circuit_latitude': location_data['latitude'],
                            'circuit_longitude': location_data['longitude'],
                            'circuit_altitude': location_data['altitude']
                        })
                
                return circuit_info
            return None
        except Exception as e:
            self.logger.warning(f"Circuit fetch failed for meeting {meeting_key}: {e}")
            return None

    def get_session_info(self, session_key):
        url = f"https://api.openf1.org/v1/sessions?session_key={session_key}"
        try:
            response = requests.get(url)
            return response.json()[0] if response.status_code == 200 and response.json() else None
        except Exception as e:
            self.logger.warning(f"Session fetch failed for key {session_key}: {e}")
            return None
    '''
    def fetch_and_send_lap_data(self):
        self.logger.info("Starting F1 Lap Stream...")
        while True:
            try:
                year = datetime.now().year
                sessions = requests.get(
                    "https://api.openf1.org/v1/sessions",
                    params={"year": year, "session_type": "Race"}
                ).json()

                for session in sessions:
                    session_key = session.get("session_key")
                    meeting_key = session.get("meeting_key")
                    circuit_info = self.get_circuit_info(meeting_key)
                    session_info = self.get_session_info(session_key)

                    if not circuit_info or not session_info:
                        continue

                    laps = requests.get(
                        "https://api.openf1.org/v1/laps",
                        params={"session_key": session_key}
                    ).json()

                    for lap in laps:
                        driver_info = self.get_driver_info(lap["driver_number"])
                        if not driver_info:
                            continue

                        processed_lap = {
                            "id": f"{session_info['year']}_{session_key}_{lap['driver_number']}_{lap['lap_number']}",
                            "race": f"{circuit_info['circuit_short_name']}_{session_info['year']}",
                            "date": session_info["date_start"],
                            "circuit": circuit_info["circuit_short_name"],
                            "latitude": circuit_info.get("circuit_latitude", None),
                            "longitude": circuit_info.get("circuit_longitude", None),
                            "altitude": circuit_info.get("circuit_altitude", None),
                            "driver": driver_info["name_acronym"],
                            "carNumber": lap["driver_number"],
                            "constructor": driver_info["team_name"],
                            "lapNumber": lap["lap_number"],
                            "lapPosition": lap.get("position", -1),
                            "pitStop": 1 if lap.get("pit_out_time") else 0,
                            "pitCount": lap.get("stint", 1),
                            "lapTime_ms": lap.get("lap_time_ms", 0),
                            "timestamp": datetime.now().isoformat()
                        }

                        self.producer.send(self.topic, value=processed_lap)
                        self.logger.info(f"Sent lap data: {processed_lap['id']}")

            except Exception as e:
                self.logger.error(f"Unexpected error: {e}")

            time.sleep(10)'''
    def fetch_and_send_lap_data(self):
        self.logger.info("Starting F1 Lap Stream...")
        
        while True:
            try:
                year = datetime.now().year
                sessions = requests.get(
                    "https://api.openf1.org/v1/sessions",
                    params={"year": year, "session_type": "Race"}
                ).json()

                for session in sessions:
                    session_key = session.get("session_key")
                    meeting_key = session.get("meeting_key")
                    circuit_info = self.get_circuit_info(meeting_key)
                    session_info = self.get_session_info(session_key)

                    if not circuit_info or not session_info:
                        continue

                    # Get all pit data for this session at once
                    pit_data = self.get_pit_data(session_key)
                    pit_times = {
                        (p['driver_number'], p['lap_number']): p['pit_duration'] * 1000  # Convert to ms
                        for p in pit_data if 'pit_duration' in p
                    }

                    laps = requests.get(
                        "https://api.openf1.org/v1/laps",
                        params={"session_key": session_key}
                    ).json()

                    for lap in laps:
                        driver_info = self.get_driver_info(lap["driver_number"])
                        if not driver_info:
                            continue

                        # Get pit time for this specific lap if available
                        pit_time = pit_times.get(
                            (lap['driver_number'], lap['lap_number']), 
                            0  # Default to 0 if no pit stop
                        )

                        processed_lap = {
                            "id": f"{session_info['year']}_{session_key}_{lap['driver_number']}_{lap['lap_number']}",
                            "race": f"{circuit_info['circuit_short_name']}_{session_info['year']}",
                            "date": session_info["date_start"],
                            "circuit": circuit_info["circuit_short_name"],
                            "latitude": circuit_info.get("circuit_latitude", None),
                            "longitude": circuit_info.get("circuit_longitude", None),
                            "altitude": circuit_info.get("circuit_altitude", None),
                            "driver": driver_info["name_acronym"],
                            "carNumber": lap["driver_number"],
                            "constructor": driver_info["team_name"],
                            "lapNumber": lap["lap_number"],
                            "lapPosition": lap.get("position", -1),
                            "pitStop": 1 if lap.get("pit_out_time") else 0,
                            "pitCount": lap.get("stint", 1),
                            "pitTime_ms": pit_time,
                            "lapTime_ms": lap.get("lap_time_ms", 0),
                            "timestamp": datetime.now().isoformat()
                        }

                        self.producer.send(self.topic, value=processed_lap)
                        self.logger.info(f"Sent lap data: {processed_lap['id']}")

            except Exception as e:
                self.logger.error(f"Unexpected error: {e}")

            time.sleep(10)

if __name__ == "__main__":
    stream = F1LapStream()
    stream.fetch_and_send_lap_data()