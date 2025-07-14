import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

vehicle_ids = ["TRUCK_001", "TRUCK_002", "VAN_001", "BIKE_003"]

NUM_MESSAGES = 10000

for _ in range(NUM_MESSAGES):
    data = {
        "vehicle_id": random.choice(vehicle_ids),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "latitude": 30.0444, 
        "speed": round(random.uniform(30, 100), 2),
        "engine_temp": round(random.uniform(70, 120), 2),
        "fuel_level": round(random.uniform(10, 100), 2)
    }

 ## kafka topic
    producer.send("vehicle-data", value=data)
    print("Sent:", data)

    # time.sleep(0.001)

print("done sending messages")
