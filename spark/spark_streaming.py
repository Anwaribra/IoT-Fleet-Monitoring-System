from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType, IntegerType
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SimpleFleetProcessor:
    def __init__(self):
        self.spark = self.create_spark_session()
        self.setup_schema()
    
    def create_spark_session(self):
        """Create Spark session with Cassandra connector"""
        return SparkSession.builder \
            .appName("SimpleIoTFleetStream") \
            .config("spark.cassandra.connection.host", "cassandra") \
            .config("spark.cassandra.connection.port", "9042") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
            .getOrCreate()
    def setup_schema(self):
        """Define telemetry schema"""
        self.telemetry_schema = StructType() \
            .add("vehicle_id", StringType()) \
            .add("vehicle_type", StringType()) \
            .add("timestamp", StringType()) \
            .add("latitude", DoubleType()) \
            .add("longitude", DoubleType()) \
            .add("speed", DoubleType()) \
            .add("engine_temp", DoubleType()) \
            .add("fuel_level", DoubleType()) \
            .add("odometer", IntegerType()) \
            .add("battery_voltage", DoubleType())
    
    def read_kafka_stream(self):
        """Read from Kafka"""
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "vehicle-telemetry") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
    def parse_data(self, df):
        """Parse JSON data from Kafka"""
        return df \
            .selectExpr("CAST(value AS STRING) as json_string") \
            .select(from_json(col("json_string"), self.telemetry_schema).alias("data")) \
            .select("data.*") \
            .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
    
    def write_to_cassandra(self, df):
        """Write to Cassandra"""
        return df.writeStream \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", "fleet_monitoring") \
            .option("table", "vehicle_telemetry") \
            .option("checkpointLocation", "/tmp/checkpoint/telemetry") \
            .outputMode("append") \
            .trigger(processingTime="10 seconds")
    
    def write_console(self, df):
        """Write to console for BIG DATA monitoring"""
        return df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .option("numRows", 5) \
            .trigger(processingTime="5 seconds")
    
    def start(self):
        """Start BIG DATA processing"""
        logger.info("Starting BIG DATA Fleet Processor - Target: 100k+ records...")
        
        try:
            # read and parse data
            raw_stream = self.read_kafka_stream()
            parsed_data = self.parse_data(raw_stream)
          
            clean_data = parsed_data.select(
                "vehicle_id", "timestamp", "latitude", "longitude",
                "speed", "engine_temp", "fuel_level"
            )
            # start Cassandra write 
            cassandra_query = self.write_to_cassandra(clean_data).start()
            console_query = self.write_console(clean_data).start()
            
            logger.info("BIG DATA Streaming started! Processing to Cassandra...")
            self.spark.streams.awaitAnyTermination()
            
        except Exception as e:
            logger.error(f"Error: {e}")
            raise
        finally:
            self.spark.stop()
if __name__ == "__main__":
    processor = SimpleFleetProcessor()
    processor.start()
