import json
import logging
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
class SimpleKafkaToCassandra:
    def __init__(self):
        self.setup_cassandra()
        self.setup_kafka()
        self.record_count = 0
    
    def setup_cassandra(self):
        """Setup Cassandra connection"""
        try:
            self.cluster = Cluster(['cassandra'], port=9042)
            self.session = self.cluster.connect()
            self.session.set_keyspace('fleet_monitoring')
            
            self.insert_stmt = self.session.prepare("""
                INSERT INTO vehicle_telemetry 
                (vehicle_id, timestamp, latitude, longitude, speed, engine_temp, fuel_level)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """)
            logger.info("Connected to Cassandra successfully!")
            
        except Exception as e:
            logger.error(f"Failed to connect to Cassandra: {e}")
            raise
    
    def setup_kafka(self):
        """Setup Kafka consumer"""
        try:
            self.consumer = KafkaConsumer(
                'vehicle-telemetry',
                bootstrap_servers=['kafka:29092'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=10000,
                auto_offset_reset='earliest'
            )
            logger.info("Connected to Kafka successfully!")
            
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
    
    def process_message(self, message):
        """Process and store a single message"""
        try:
            data = message
            
           
            timestamp = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
            
            # Execute insert
            self.session.execute(self.insert_stmt, [
                data['vehicle_id'],
                timestamp,
                data['latitude'],
                data['longitude'],
                data['speed'],
                data['engine_temp'],
                data['fuel_level']
            ])
            
            self.record_count += 1
            
            if self.record_count % 100 == 0:
                logger.info(f"BIG DATA STORED: {self.record_count} records in Cassandra!")
                
        except Exception as e:
            logger.error(f"Failed to process message: {e}")
    
    def consume_and_store(self):
        """Main consumer loop"""
        logger.info("Starting BIG DATA Kafka to Cassandra consumer...")
        logger.info("Processing 100k+ messages from Kafka to Cassandra...")
        try:
            for message in self.consumer:
                self.process_message(message.value)
                
                if self.record_count % 1000 == 0:
                    logger.info(f"PROGRESS: {self.record_count} records stored - Target: 30k+")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            logger.info(f"FINAL COUNT: {self.record_count} records stored in Cassandra!")
            self.consumer.close()
            self.cluster.shutdown()

if __name__ == "__main__":
    processor = SimpleKafkaToCassandra()
    processor.consume_and_store() 