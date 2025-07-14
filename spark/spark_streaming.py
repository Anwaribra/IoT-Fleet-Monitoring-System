from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

#create spark session
spark = SparkSession.builder \
    .appName("FleetMonitoringStream") \
    .getOrCreate()

#  Define schema for JSON messages
schema = StructType() \
    .add("vehicle_id", StringType()) \
    .add("timestamp", StringType()) \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("speed", DoubleType()) \
    .add("engine_temp", DoubleType()) \
    .add("fuel_level", DoubleType())

# Read stream from Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "vehicle-data") \
    .option("startingOffsets", "latest") \
    .load()

#  Convert Kafka message from binary → JSON
json_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

#Filter overheating trucks
alerts_df = json_df.filter(col("engine_temp") > 100)

#Output to console
query = alerts_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
