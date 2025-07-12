import time
import json
import random
import paho.mqtt.client as mqtt
from datetime import datetime

client = mqtt.Client()
client.connect("localhost", 1883, 60)


TOTAL_MESSAGES = 10000

for i in range(TOTAL_MESSAGES):
    payload = {
        "vehicle_id": "TRUCK_001",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "latitude": 30.0444,
        "longitude": 31.2357,
        "speed": round(random.uniform(40, 90), 2),
        "engine_temp": round(random.uniform(70, 110), 2),
        "fuel_level": round(random.uniform(20, 80), 2)
    }

    client.publish("fleet/TRUCK_001/sensors", json.dumps(payload))
    
    with open("data/data_log.jsonl", "a") as f:
        f.write(json.dumps(payload) + "\n")

    print(f"Published #{i + 1}: {payload}")
    time.sleep(0.01)
