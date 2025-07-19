import time
import json
import random
import paho.mqtt.client as mqtt
from datetime import datetime, timedelta
import math
import os
import logging

# configure 
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class VehicleSimulator:
    def __init__(self, mqtt_broker_host="localhost", mqtt_broker_port=1883):
        self.client = mqtt.Client()
        self.broker_host = mqtt_broker_host
        self.broker_port = mqtt_broker_port
        self.connect_to_broker()
        
        self.fleet = {
            "TRUCK_001": {
                "type": "truck",
                "route": [(30.0444, 31.2357), (30.0500, 31.2400), (30.0600, 31.2500)],
                "current_pos": 0,
                "fuel_consumption_rate": 0.1,
                "base_engine_temp": 85,
                "max_speed": 90
            },
            "TRUCK_002": {
                "type": "truck", 
                "route": [(30.0300, 31.2200), (30.0400, 31.2300), (30.0450, 31.2350)],
                "current_pos": 0,
                "fuel_consumption_rate": 0.12,
                "base_engine_temp": 80,
                "max_speed": 85
            },
            "TRUCK_003": {
                "type": "truck", 
                "route": [(30.0550, 31.2450), (30.0650, 31.2550), (30.0750, 31.2650)],
                "current_pos": 0,
                "fuel_consumption_rate": 0.11,
                "base_engine_temp": 82,
                "max_speed": 88
            },
            "VAN_001": {
                "type": "van",
                "route": [(30.0200, 31.2100), (30.0250, 31.2150), (30.0300, 31.2200)],
                "current_pos": 0,
                "fuel_consumption_rate": 0.08,
                "base_engine_temp": 75,
                "max_speed": 100
            },
            "VAN_002": {
                "type": "van",
                "route": [(30.0150, 31.2050), (30.0200, 31.2100), (30.0250, 31.2150)],
                "current_pos": 0,
                "fuel_consumption_rate": 0.09,
                "base_engine_temp": 78,
                "max_speed": 95
            },
            "BUS_001": {
                "type": "bus",
                "route": [(30.0100, 31.2000), (30.0200, 31.2100), (30.0300, 31.2200)],
                "current_pos": 0,
                "fuel_consumption_rate": 0.15,
                "base_engine_temp": 90,
                "max_speed": 70
            },
            "BIKE_001": {
                "type": "motorcycle",
                "route": [(30.0100, 31.2000), (30.0150, 31.2050), (30.0200, 31.2100)],
                "current_pos": 0,
                "fuel_consumption_rate": 0.05,
                "base_engine_temp": 70,
                "max_speed": 120
            },
            "BIKE_002": {
                "type": "motorcycle",
                "route": [(30.0080, 31.1980), (30.0130, 31.2030), (30.0180, 31.2080)],
                "current_pos": 0,
                "fuel_consumption_rate": 0.04,
                "base_engine_temp": 68,
                "max_speed": 125
            },
            "CAR_001": {
                "type": "car",
                "route": [(30.0350, 31.2250), (30.0400, 31.2300), (30.0450, 31.2350)],
                "current_pos": 0,
                "fuel_consumption_rate": 0.07,
                "base_engine_temp": 72,
                "max_speed": 110
            },
            "CAR_002": {
                "type": "car",
                "route": [(30.0320, 31.2220), (30.0370, 31.2270), (30.0420, 31.2320)],
                "current_pos": 0,
                "fuel_consumption_rate": 0.06,
                "base_engine_temp": 74,
                "max_speed": 115
            }
        }
        
        for vehicle_id in self.fleet:
            self.fleet[vehicle_id]["fuel_level"] = random.uniform(70, 100)
            self.fleet[vehicle_id]["speed"] = 0
            self.fleet[vehicle_id]["engine_temp"] = self.fleet[vehicle_id]["base_engine_temp"]
    
    def connect_to_broker(self):
        """connect to mqtt broker with retry logic"""
        max_retries = 5
        for attempt in range(max_retries):
            try:
                self.client.connect(self.broker_host, self.broker_port, 60)
                logger.info(f"Connected to mqtt broker at {self.broker_host}:{self.broker_port}")
                return
            except Exception as e:
                logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  
                else:
                    logger.error("Failed to connect to MQTT broker after all retries")
                    raise
    
    def calculate_position(self, vehicle_id):
        """calculate realistic GPS position along route"""
        vehicle = self.fleet[vehicle_id]
        route = vehicle["route"]
        current_pos = vehicle["current_pos"]
        
        # route progression
        if len(route) > 1:
            progress = (current_pos % 100) / 100.0
            current_waypoint = int(current_pos / 100) % len(route)
            next_waypoint = (current_waypoint + 1) % len(route)
            
            lat1, lon1 = route[current_waypoint]
            lat2, lon2 = route[next_waypoint]
            
            lat = lat1 + (lat2 - lat1) * progress + random.uniform(-0.001, 0.001)
            lon = lon1 + (lon2 - lon1) * progress + random.uniform(-0.001, 0.001)
            
            vehicle["current_pos"] += 1
        else:
            lat, lon = route[0]
            lat += random.uniform(-0.002, 0.002)
            lon += random.uniform(-0.002, 0.002)
        
        return round(lat, 6), round(lon, 6)
    
    def simulate_realistic_data(self, vehicle_id):
        """generate realistic sensor data with correlations"""
        vehicle = self.fleet[vehicle_id]

        base_speed = random.uniform(30, vehicle["max_speed"])

        current_hour = datetime.now().hour
        if 7 <= current_hour <= 9 or 17 <= current_hour <= 19:
            base_speed *= random.uniform(0.5, 0.8)  
        
        vehicle["speed"] = round(base_speed, 2)
        
        temp_increase = (base_speed / vehicle["max_speed"]) * 20
        ambient_temp_var = random.uniform(-5, 10)  
        vehicle["engine_temp"] = round(
            vehicle["base_engine_temp"] + temp_increase + ambient_temp_var, 2
        )

        consumption = vehicle["fuel_consumption_rate"] * (base_speed / 60)
        vehicle["fuel_level"] = max(0, vehicle["fuel_level"] - consumption)

        if vehicle["fuel_level"] < 15:
            vehicle["fuel_level"] = random.uniform(80, 100)
        
        return vehicle["speed"], vehicle["engine_temp"], round(vehicle["fuel_level"], 2)
    
    def generate_telemetry(self, vehicle_id):
        """generate complete telemetry data for a vehicle"""
        lat, lon = self.calculate_position(vehicle_id)
        speed, engine_temp, fuel_level = self.simulate_realistic_data(vehicle_id)
        
        payload = {
            "vehicle_id": vehicle_id,
            "vehicle_type": self.fleet[vehicle_id]["type"],
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "latitude": lat,
            "longitude": lon,
            "speed": speed,
            "engine_temp": engine_temp,
            "fuel_level": fuel_level,
            "odometer": random.randint(50000, 200000),  
            "battery_voltage": round(random.uniform(12.0, 14.4), 2)
        }
        
        return payload
    
    def publish_telemetry(self, vehicle_id, payload):
        """Publish telemetry to MQTT broker"""
        topic = f"fleet/{vehicle_id}/telemetry"
        try:
            result = self.client.publish(topic, json.dumps(payload))
            if result.rc == 0:
                logger.debug(f"Published to {topic}: {payload}")
            else:
                logger.warning(f"Failed to publish to {topic}, return code: {result.rc}")
        except Exception as e:
            logger.error(f"Error publishing to {topic}: {e}")
    
    def log_to_file(self, payload):
        """Log telemetry data to file for debugging"""
        data_dir = "/data"
        os.makedirs(data_dir, exist_ok=True)
        with open(f"{data_dir}/telemetry_log.jsonl", "a") as f:
            f.write(json.dumps(payload) + "\n")
    
    def run_simulation(self, duration_minutes=60, interval_seconds=1):
        """Run the fleet simulation - FAST for 100k records"""
        logger.info(f"Starting BIG DATA simulation for {duration_minutes} minutes")
        logger.info(f"Simulating {len(self.fleet)} vehicles: {list(self.fleet.keys())}")
        
        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=duration_minutes)
        message_count = 0
        
        try:
            while datetime.now() < end_time:

                for batch in range(5):  
                    for vehicle_id in self.fleet:
                        payload = self.generate_telemetry(vehicle_id)
                        self.publish_telemetry(vehicle_id, payload)
                        message_count += 1
                    
                    if message_count % 1000 == 0:
                        logger.info(f"BIG DATA: Sent {message_count} messages - Target: 100k+")
                    
                    time.sleep(0.2)  
                
                time.sleep(interval_seconds)
        
        except KeyboardInterrupt:
            logger.info("simulation interrupted by user")
        
        finally:
            elapsed = (datetime.now() - start_time).total_seconds()
            logger.info(f"simulation completed. Sent {message_count} messages in {elapsed:.1f} seconds")
            self.client.disconnect()

if __name__ == "__main__":
    
    MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
    MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
    SIMULATION_DURATION = int(os.getenv("SIMULATION_DURATION_MINUTES", "60"))
    MESSAGE_INTERVAL = float(os.getenv("MESSAGE_INTERVAL_SECONDS", "5"))

    simulator = VehicleSimulator(MQTT_BROKER, MQTT_PORT)
    simulator.run_simulation(SIMULATION_DURATION, MESSAGE_INTERVAL)
