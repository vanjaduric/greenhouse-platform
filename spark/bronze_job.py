"""
Bronze Layer - Raw Landing
Reads from all Kafka sensor topics, adds ingestion metadata,
writes raw Parquet to MinIO partitioned by facility/date/hour.

"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, to_date, hour, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, BooleanType, TimestampType
)
import os

# Config
# -------------------------------------------------------------------------------------

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY  = os.getenv("MINIO_SECRET_KEY")
BRONZE_PATH       = os.getenv("BRONZE_PATH")
CHECKPOINT_PATH   = os.getenv("CHECKPOINT_PATH")

SENSOR_TOPICS = [
    "sensors.temperature",
    "sensors.humidity",
    "sensors.co2",
    "sensors.light",
    "sensors.airflow"
]

IRRIGATION_TOPIC = "events.irrigation"
DLQ_TOPIC        = "dead.letter.queue"

SENSOR_SCHEMA = StructType([
    StructField("sensor_id",             StringType(),  True),
    StructField("value",                 DoubleType(),  True),
    StructField("unit",                  StringType(),  True),
    StructField("timestamp",             StringType(),  True),
    StructField("anomaly_injected",      StringType(),  True),  # spike/drift/dropout/null
    StructField("is_peer_outlier",       BooleanType(), True),
    StructField("peer_deviation",        DoubleType(),  True),
    StructField("facility_id",           StringType(),  True),
    StructField("crop_type",             StringType(),  True),
    StructField("growth_week",           IntegerType(), True),
    StructField("ingested_at",           StringType(),  True),
    StructField("facility_aggregate",    DoubleType(),  True),
    StructField("sensor_count",          IntegerType(), True),
    StructField("healthy_sensor_count",  IntegerType(), True),
])

IRRIGATION_SCHEMA = StructType([
    StructField("zone_id",             StringType(), True),
    StructField("started_at",          StringType(), True),
    StructField("duration_seconds",    IntegerType(), True),
    StructField("volume_liters",       DoubleType(),  True),
    StructField("water_temp_celsius",  DoubleType(),  True),
    StructField("facility_id",         StringType(), True),
    StructField("crop_type",           StringType(), True),
    StructField("ingested_at",         StringType(), True),
])


spark = SparkSession.builder \
    .appName("greenhouse-bronze") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint",            MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key",          MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key",          MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access",   "true") \
    .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.shuffle.partitions",          "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark session started")

def read_kafka(topics):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", ",".join(topics)) \
        .option("startingOffsets", "earliest") \
        .load()

# Sensor stream
# -------------------------------------------------------------------------------------

sensor_raw = read_kafka(SENSOR_TOPICS)

sensor_parsed = sensor_raw.select(
    from_json(col("value").cast("string"), SENSOR_SCHEMA).alias("d"),
    col("topic").alias("source_topic"),
    col("partition"),
    col("offset")
).select(
    "d.*",
    "source_topic",
    "partition",
    "offset",
    current_timestamp().alias("bronze_landed_at")
).withColumn("date", to_date(col("bronze_landed_at"))) \
 .withColumn("hour", hour(col("bronze_landed_at")))

sensor_query = sensor_parsed.coalesce(1).writeStream \
    .format("parquet") \
    .option("path",               f"{BRONZE_PATH}/sensors") \
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/sensors") \
    .partitionBy("facility_id", "date", "hour") \
    .trigger(processingTime="30 seconds") \
    .outputMode("append") \
    .start()

# Irrigation stream
# -------------------------------------------------------------------------------------

irrigation_raw = read_kafka([IRRIGATION_TOPIC])

irrigation_parsed = irrigation_raw.select(
    from_json(col("value").cast("string"), IRRIGATION_SCHEMA).alias("d"),
    col("topic").alias("source_topic"),
    current_timestamp().alias("bronze_landed_at")
).select(
    "d.*",
    "source_topic",
    "bronze_landed_at"
).withColumn("date", to_date(col("bronze_landed_at")))

irrigation_query = irrigation_parsed.coalesce(1).writeStream \
    .format("parquet") \
    .option("path",               f"{BRONZE_PATH}/irrigation") \
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/irrigation") \
    .partitionBy("facility_id", "date") \
    .trigger(processingTime="30 seconds") \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()
