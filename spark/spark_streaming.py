from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, max as spark_max, min as spark_min, 
    count, sum as spark_sum, when, lit, udf, current_timestamp,
    to_timestamp, date_format, uuid as spark_uuid
)
from pyspark.sql.types import (
    StructType, StringType, DoubleType, TimestampType, IntegerType, BooleanType
)
import logging
import os
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FleetStreamProcessor:
    def __init__(self):
        self.spark = self.create_spark_session()
        self.setup_schemas()
        self.setup_cassandra_config()
    
    def create_spark_session(self):
        """Create Spark session with necessary configurations"""
        return SparkSession.builder \
            .appName("IoTFleetMonitoringStream") \
            .config("spark.cassandra.connection.host", "localhost") \
            .config("spark.cassandra.connection.port", "9042") \
            .config("spark.cassandra.auth.username", "cassandra") \
            .config("spark.cassandra.auth.password", "cassandra") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
    
    def setup_schemas(self):
        """Define schemas for different message types"""
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
            .add("battery_voltage", DoubleType()) \
            .add("message_type", StringType()) \
            .add("processed_at", StringType())
    
    def setup_cassandra_config(self):
        """Setup Cassandra connection configuration"""
        self.cassandra_keyspace = "fleet_monitoring"
        self.cassandra_tables = {
            "telemetry": "vehicle_telemetry",
            "metrics": "vehicle_metrics_hourly", 
            "alerts": "vehicle_alerts",
            "fleet_summary": "fleet_summary"
        }
    
    def read_kafka_stream(self, topic="vehicle-telemetry"):
        """Read streaming data from Kafka"""
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
    
    def parse_telemetry_data(self, raw_df):
        """Parse JSON telemetry data from Kafka"""
        return raw_df \
            .selectExpr("CAST(value AS STRING) as json_string") \
            .select(from_json(col("json_string"), self.telemetry_schema).alias("data")) \
            .select("data.*") \
            .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")) \
            .withColumn("processed_timestamp", current_timestamp())
    
    def detect_anomalies(self, df):
        """Detect various anomalies in vehicle data"""
        
        # Define anomaly detection UDFs
        @udf(returnType=BooleanType())
        def is_high_temp_anomaly(engine_temp):
            return engine_temp > 110.0
        
        @udf(returnType=BooleanType())
        def is_low_fuel_anomaly(fuel_level):
            return fuel_level < 10.0
        
        @udf(returnType=BooleanType())
        def is_high_speed_anomaly(speed, vehicle_type):
            speed_limits = {"truck": 90, "van": 100, "motorcycle": 120}
            return speed > speed_limits.get(vehicle_type, 100)
        
        @udf(returnType=BooleanType())
        def is_battery_anomaly(battery_voltage):
            return battery_voltage < 11.5 or battery_voltage > 15.0
        
        # Apply anomaly detection
        anomaly_df = df.withColumn(
            "high_temp_alert", is_high_temp_anomaly(col("engine_temp"))
        ).withColumn(
            "low_fuel_alert", is_low_fuel_anomaly(col("fuel_level"))
        ).withColumn(
            "high_speed_alert", is_high_speed_anomaly(col("speed"), col("vehicle_type"))
        ).withColumn(
            "battery_alert", is_battery_anomaly(col("battery_voltage"))
        )
        
        # Filter records with any anomaly
        return anomaly_df.filter(
            col("high_temp_alert") | 
            col("low_fuel_alert") | 
            col("high_speed_alert") | 
            col("battery_alert")
        )
    
    def create_alerts_from_anomalies(self, anomaly_df):
        """Convert anomalies to alert records"""
        
        # Create separate alert records for each anomaly type
        alerts = []
        
        # High temperature alerts
        high_temp_alerts = anomaly_df.filter(col("high_temp_alert")) \
            .select(
                spark_uuid().alias("alert_id"),
                col("vehicle_id"),
                lit("HIGH_ENGINE_TEMP").alias("alert_type"),
                col("engine_temp").cast(StringType()).alias("alert_message"),
                lit("WARNING").alias("severity"),
                col("timestamp"),
                lit(False).alias("resolved")
            )
        
        # Low fuel alerts
        low_fuel_alerts = anomaly_df.filter(col("low_fuel_alert")) \
            .select(
                spark_uuid().alias("alert_id"),
                col("vehicle_id"),
                lit("LOW_FUEL").alias("alert_type"),
                col("fuel_level").cast(StringType()).alias("alert_message"),
                lit("CRITICAL").alias("severity"),
                col("timestamp"),
                lit(False).alias("resolved")
            )
        
        # High speed alerts
        high_speed_alerts = anomaly_df.filter(col("high_speed_alert")) \
            .select(
                spark_uuid().alias("alert_id"),
                col("vehicle_id"),
                lit("EXCESSIVE_SPEED").alias("alert_type"),
                col("speed").cast(StringType()).alias("alert_message"),
                lit("WARNING").alias("severity"),
                col("timestamp"),
                lit(False).alias("resolved")
            )
        
        # Battery alerts
        battery_alerts = anomaly_df.filter(col("battery_alert")) \
            .select(
                spark_uuid().alias("alert_id"),
                col("vehicle_id"),
                lit("BATTERY_ISSUE").alias("alert_type"),
                col("battery_voltage").cast(StringType()).alias("alert_message"),
                lit("WARNING").alias("severity"),
                col("timestamp"),
                lit(False).alias("resolved")
            )
        
        # Union all alert types
        all_alerts = high_temp_alerts.union(low_fuel_alerts) \
                                   .union(high_speed_alerts) \
                                   .union(battery_alerts)
        
        return all_alerts
    
    def aggregate_metrics(self, df):
        """Create hourly aggregated metrics for each vehicle"""
        return df.groupBy(
            col("vehicle_id"),
            window(col("timestamp"), "1 hour").alias("hour_window")
        ).agg(
            avg("speed").alias("avg_speed"),
            spark_max("speed").alias("max_speed"),
            spark_min("speed").alias("min_speed"),
            avg("engine_temp").alias("avg_engine_temp"),
            spark_max("engine_temp").alias("max_engine_temp"),
            avg("fuel_level").alias("avg_fuel_level"),
            spark_min("fuel_level").alias("min_fuel_level"),
            count("*").alias("total_records")
        ).select(
            col("vehicle_id"),
            col("hour_window.start").alias("hour"),
            col("avg_speed"),
            col("max_speed"),
            col("min_speed"),
            col("avg_engine_temp"),
            col("max_engine_temp"),
            col("avg_fuel_level"),
            col("min_fuel_level"),
            lit(0.0).alias("distance_traveled"),  # Would need GPS calculation
            col("total_records")
        )
    
    def write_to_cassandra(self, df, table_name):
        """Write DataFrame to Cassandra table"""
        return df.writeStream \
            .format("org.apache.spark.sql.cassandra") \
            .option("keyspace", self.cassandra_keyspace) \
            .option("table", table_name) \
            .option("checkpointLocation", f"/tmp/checkpoint/{table_name}") \
            .outputMode("append") \
            .trigger(processingTime="30 seconds")
    
    def write_to_console(self, df, query_name):
        """Write DataFrame to console for debugging"""
        return df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .option("numRows", 20) \
            .queryName(query_name) \
            .trigger(processingTime="30 seconds")
    
    def start_processing(self):
        """Start the main streaming processing pipeline"""
        logger.info("Starting IoT Fleet Monitoring Stream Processor...")
        
        try:
            # Read raw telemetry stream
            raw_telemetry = self.read_kafka_stream("vehicle-telemetry")
            parsed_telemetry = self.parse_telemetry_data(raw_telemetry)
            
            # Store raw telemetry to Cassandra
            telemetry_query = self.write_to_cassandra(
                parsed_telemetry.select(
                    "vehicle_id", "timestamp", "latitude", "longitude",
                    "speed", "engine_temp", "fuel_level"
                ),
                self.cassandra_tables["telemetry"]
            ).start()
            
            # Detect anomalies and create alerts
            anomalies = self.detect_anomalies(parsed_telemetry)
            alerts = self.create_alerts_from_anomalies(anomalies)
            
            # Write alerts to Cassandra
            alerts_query = self.write_to_cassandra(
                alerts,
                self.cassandra_tables["alerts"]
            ).start()
            
            # Create hourly aggregations
            hourly_metrics = self.aggregate_metrics(parsed_telemetry)
            
            # Write metrics to Cassandra
            metrics_query = self.write_to_cassandra(
                hourly_metrics,
                self.cassandra_tables["metrics"]
            ).start()
            
            # Console outputs for monitoring
            console_telemetry = self.write_to_console(
                parsed_telemetry.select("vehicle_id", "timestamp", "speed", "engine_temp", "fuel_level"),
                "telemetry_monitor"
            ).start()
            
            console_alerts = self.write_to_console(
                alerts,
                "alerts_monitor"
            ).start()
            
            logger.info("All streaming queries started successfully")
            
            # Wait for termination
            self.spark.streams.awaitAnyTermination()
            
        except Exception as e:
            logger.error(f"Error in streaming processing: {e}")
            raise
        finally:
            logger.info("Stopping stream processor...")
            self.spark.stop()

if __name__ == "__main__":
    processor = FleetStreamProcessor()
    processor.start_processing()
