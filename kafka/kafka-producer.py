import json
import logging
import os
import time
from datetime import datetime
from typing import Dict, Any

import paho.mqtt.client as mqtt
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MQTTKafkaBridge:
    def __init__(self, 
                 mqtt_broker_host="localhost", 
                 mqtt_broker_port=1883,
                 kafka_bootstrap_servers="localhost:9092"):
        
        self.mqtt_broker_host = mqtt_broker_host
        self.mqtt_broker_port = mqtt_broker_port
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_connect = self.on_mqtt_connect
        self.mqtt_client.on_message = self.on_mqtt_message
        self.mqtt_client.on_disconnect = self.on_mqtt_disconnect
        
        self.kafka_producer = self.create_kafka_producer()
        
        self.messages_received = 0
        self.messages_sent = 0
        self.last_stats_time = time.time()
        
        # MQTT topic pattern -> kafka topic
        self.topic_mapping = {
            "fleet/+/telemetry": "vehicle-telemetry",
            "fleet/+/alerts": "vehicle-alerts",
            "fleet/+/diagnostics": "vehicle-diagnostics"
        }
    def create_kafka_producer(self) -> KafkaProducer:
        """Create Kafka producer with proper configuration"""
        producer_config = {
            'bootstrap_servers': self.kafka_bootstrap_servers,
            'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
            'key_serializer': lambda k: k.encode('utf-8') if k else None,
            'acks': 'all',  
            'retries': 3,
            'batch_size': 16384,
            'linger_ms': 10,  
            'buffer_memory': 33554432,
            'compression_type': 'gzip'
        }
        
        try:
            producer = KafkaProducer(**producer_config)
            logger.info("Kafka producer created successfully")
            return producer
        except Exception as e:
            logger.error(f"Failed to create Kafka producer: {e}")
            raise
    def on_mqtt_connect(self, client, userdata, flags, rc):
        """Callback for MQTT connection"""
        if rc == 0:
            logger.info(f"Connected to MQTT broker at {self.mqtt_broker_host}:{self.mqtt_broker_port}")

            for mqtt_topic in self.topic_mapping.keys():
                client.subscribe(mqtt_topic)
                logger.info(f"Subscribed to MQTT topic: {mqtt_topic}")
        else:
            logger.error(f"Failed to connect to MQTT broker, return code: {rc}")
    
    def on_mqtt_disconnect(self, client, userdata, rc):
        """Callback for MQTT disconnection"""
        if rc != 0:
            logger.warning("Unexpected MQTT disconnection. Attempting to reconnect...")
        else:
            logger.info("MQTT client disconnected")
    
    def on_mqtt_message(self, client, userdata, msg):
        """Process incoming MQTT messages and forward to Kafka"""
        try:
            self.messages_received += 1
            topic = msg.topic
            payload_str = msg.payload.decode('utf-8')
            payload = json.loads(payload_str)
            kafka_topic = self.get_kafka_topic(topic)
            if not kafka_topic:
                logger.warning(f"No Kafka topic mapping found for MQTT topic: {topic}")
                return
            vehicle_id = self.extract_vehicle_id(topic, payload)
            enriched_payload = self.enrich_payload(payload, topic)
            # send to kafka
            self.send_to_kafka(kafka_topic, enriched_payload, vehicle_id)
            self.log_statistics()
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON from MQTT message: {e}")
        except Exception as e:
            logger.error(f"Error processing MQTT message: {e}")
    
    def get_kafka_topic(self, mqtt_topic: str) -> str:
        """Map MQTT topic to Kafka topic"""
        for pattern, kafka_topic in self.topic_mapping.items():
            # Simple pattern matching (+ is single level wildcard)
            pattern_parts = pattern.split('/')
            topic_parts = mqtt_topic.split('/')
            
            if len(pattern_parts) == len(topic_parts):
                match = True
                for i, part in enumerate(pattern_parts):
                    if part != '+' and part != topic_parts[i]:
                        match = False
                        break
                if match:
                    return kafka_topic
        
        return None
    def extract_vehicle_id(self, mqtt_topic: str, payload: Dict[str, Any]) -> str:
        """Extract vehicle ID for Kafka partitioning"""
        # Try to get from payload first
        if 'vehicle_id' in payload:
            return payload['vehicle_id']
        topic_parts = mqtt_topic.split('/')
        if len(topic_parts) >= 2 and topic_parts[0] == 'fleet':
            return topic_parts[1]
        
        return "unknown"
    def enrich_payload(self, payload: Dict[str, Any], mqtt_topic: str) -> Dict[str, Any]:
        """Enrich payload with additional metadata"""
        enriched = payload.copy()
        
        # Add processing metadata
        enriched['mqtt_topic'] = mqtt_topic
        enriched['processed_at'] = datetime.utcnow().isoformat() + "Z"
        enriched['bridge_version'] = "1.0.0"
        
        # Add topic-specific enrichments
        if 'telemetry' in mqtt_topic:
            enriched['message_type'] = 'telemetry'
        elif 'alerts' in mqtt_topic:
            enriched['message_type'] = 'alert'
        elif 'diagnostics' in mqtt_topic:
            enriched['message_type'] = 'diagnostic'
        
        return enriched
    def send_to_kafka(self, kafka_topic: str, payload: Dict[str, Any], key: str = None):
        """Send message to Kafka with error handling"""
        try:
            future = self.kafka_producer.send(
                kafka_topic, 
                value=payload, 
                key=key
            )
            future.add_callback(self.on_kafka_success, kafka_topic, key)
            future.add_errback(self.on_kafka_error, kafka_topic, key, payload)
            
            self.messages_sent += 1
            
        except Exception as e:
            logger.error(f"Failed to send message to Kafka topic {kafka_topic}: {e}")
    
    def on_kafka_success(self, kafka_topic: str, key: str, record_metadata):
        """Callback for successful Kafka send"""
        logger.debug(f"Message sent to {kafka_topic} partition {record_metadata.partition} offset {record_metadata.offset}")
    def on_kafka_error(self, kafka_topic: str, key: str, payload: Dict[str, Any], exception):
        """Callback for failed Kafka send"""
        logger.error(f"Failed to send message to {kafka_topic} with key {key}: {exception}")

    def log_statistics(self):
        """Log performance statistics for BIG DATA"""
        current_time = time.time()
        if current_time - self.last_stats_time >= 10:  # Every 10 seconds for BIG DATA
            rate = self.messages_received / (current_time - self.last_stats_time) if (current_time - self.last_stats_time) > 0 else 0
            logger.info(
                f"BIG DATA Bridge - Received: {self.messages_received}, "
                f"Sent to Kafka: {self.messages_sent}, "
                f"Rate: {rate:.0f} msg/s - Target: 100k+ records"
            )
            self.last_stats_time = current_time
    
    def start(self):
        """Start the MQTT-Kafka bridge"""
        logger.info("Starting MQTT-Kafka bridge...")
        
        try:
            self.mqtt_client.connect(self.mqtt_broker_host, self.mqtt_broker_port, 60)
            self.mqtt_client.loop_start()
            logger.info("Bridge started successfully. Press Ctrl+C to stop.")
            while True:
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("Shutting down bridge...")
            self.stop()
        except Exception as e:
            logger.error(f"Bridge error: {e}")
            self.stop()
    
    def stop(self):
        """Stop the MQTT-Kafka bridge"""
        logger.info("Stopping MQTT-Kafka bridge...")

        self.mqtt_client.loop_stop()
        self.mqtt_client.disconnect()

        self.kafka_producer.flush()
        self.kafka_producer.close()
        
        logger.info("Bridge stopped successfully")

if __name__ == "__main__":
    MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
    MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
    KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
  
    bridge = MQTTKafkaBridge(
        mqtt_broker_host=MQTT_BROKER,
        mqtt_broker_port=MQTT_PORT,
        kafka_bootstrap_servers=KAFKA_SERVERS
    )
    bridge.start()
