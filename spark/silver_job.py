"""
Silver Layer — Cleaned, Validated & Enriched
Reads Bronze Parquet from MinIO, cleans data, calculates VPD,
flags anomalies, writes Silver Delta to MinIO.
Triggered by Airflow every 30 minutes.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, isnan, isnull, stddev, mean, abs as spark_abs,
    current_timestamp, lit, exp, round as spark_round,
    to_timestamp, lag, unix_timestamp
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP")
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BRONZE_PATH      = os.getenv("BRONZE_PATH")
SILVER_PATH      = os.getenv("SILVER_PATH")
CHECKPOINT_PATH  = os.getenv("CHECKPOINT_PATH")

# Crop-specific optimal ranges per growth week
# Silver enriches each reading with whether it's within optimal range
OPTIMAL_RANGES = {
    "tomato": {
        "temperature": (20, 26),
        "humidity":    (65, 75),
        "co2":         (800, 1100),
        "light":       (200, 600),
        "airflow":     (0.2, 0.8)
    },
    "cucumber": {
        "temperature": (22, 28),
        "humidity":    (70, 80),
        "co2":         (700, 1000),
        "light":       (150, 500),
        "airflow":     (0.2, 0.7)
    }
}

spark = SparkSession.builder \
    .appName("greenhouse-silver") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "io.delta:delta-spark_2.12:3.1.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Silver job started")

bronze_df = spark.read.parquet(BRONZE_PATH)
print(f"Bronze records read: {bronze_df.count()}")

# ---------------------------------------------------------------------------
# Step 1 — Cast and normalize
# ---------------------------------------------------------------------------

silver_df = bronze_df \
    .withColumn("value", col("value").cast(DoubleType())) \
    .withColumn("timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("ingested_at", to_timestamp(col("ingested_at")))

# ---------------------------------------------------------------------------
# Step 2 — Flag nulls (dropouts) — don't drop, flag
# ---------------------------------------------------------------------------

silver_df = silver_df.withColumn(
    "is_dropout",
    isnull(col("value")) | isnan(col("value"))
)

# ---------------------------------------------------------------------------
# Step 3 — Flag statistical anomalies per sensor
# Compare each reading against the rolling mean ± 3 std devs
# ---------------------------------------------------------------------------

sensor_window = Window.partitionBy("facility_id", "sensor_id")

silver_df = silver_df \
    .withColumn("sensor_mean",  mean(col("value")).over(sensor_window)) \
    .withColumn("sensor_stddev", stddev(col("value")).over(sensor_window)) \
    .withColumn(
        "is_statistical_anomaly",
        when(col("is_dropout"), False)
        .when(col("sensor_stddev").isNull(), False)
        .when(
            spark_abs(col("value") - col("sensor_mean")) > 3 * col("sensor_stddev"),
            True
        )
        .otherwise(False)
    )

# ---------------------------------------------------------------------------
# Step 4 — Flag range anomalies (outside crop-specific optimal range)
# We use a wider "physiologically impossible" range here —
# optimal range flagging happens in Gold
# ---------------------------------------------------------------------------

silver_df = silver_df.withColumn(
    "is_range_anomaly",
    when(col("source_topic") == "sensors.temperature",
         (col("value") < 5) | (col("value") > 50))
    .when(col("source_topic") == "sensors.humidity",
         (col("value") < 0) | (col("value") > 100))
    .when(col("source_topic") == "sensors.co2",
         (col("value") < 200) | (col("value") > 5000))
    .when(col("source_topic") == "sensors.light",
         (col("value") < 0) | (col("value") > 2000))
    .when(col("source_topic") == "sensors.airflow",
         (col("value") < 0) | (col("value") > 5))
    .otherwise(False)
)

# ---------------------------------------------------------------------------
# Step 5 — Combined anomaly flag
# ---------------------------------------------------------------------------

silver_df = silver_df.withColumn(
    "is_anomaly",
    col("is_dropout") |
    col("is_statistical_anomaly") |
    col("is_range_anomaly") |
    col("anomaly_injected").isNotNull()
)

# ---------------------------------------------------------------------------
# Step 6 — Calculate VPD for temperature + humidity pairs
# VPD = (1 - RH/100) * 0.6108 * e^(17.27 * T / (T + 237.3))
# Only meaningful when both temperature and humidity are clean
# We join temp and humidity readings on facility + timestamp window
# ---------------------------------------------------------------------------

from pyspark.sql.functions import floor, window

# Get temperature readings
temp_df = silver_df \
    .filter(col("source_topic") == "sensors.temperature") \
    .filter(~col("is_anomaly")) \
    .select(
        col("facility_id"),
        col("timestamp"),
        col("value").alias("temperature")
    )

# Get humidity readings
hum_df = silver_df \
    .filter(col("source_topic") == "sensors.humidity") \
    .filter(~col("is_anomaly")) \
    .select(
        col("facility_id"),
        col("timestamp"),
        col("value").alias("humidity")
    )

# Join on facility + nearest timestamp (within 60 seconds)
from pyspark.sql.functions import expr

vpd_df = temp_df.alias("t").join(
    hum_df.alias("h"),
    (col("t.facility_id") == col("h.facility_id")) &
    (spark_abs(
        unix_timestamp(col("t.timestamp")) -
        unix_timestamp(col("h.timestamp"))
    ) <= 60),
    "inner"
).select(
    col("t.facility_id"),
    col("t.timestamp"),
    col("t.temperature"),
    col("h.humidity"),
    spark_round(
        (1 - col("h.humidity") / 100) *
        0.6108 *
        exp(17.27 * col("t.temperature") /
            (col("t.temperature") + 237.3)),
        4
    ).alias("vpd_kpa")
)

print(f"VPD records calculated: {vpd_df.count()}")

# ---------------------------------------------------------------------------
# Step 7 — Add silver metadata
# ---------------------------------------------------------------------------

silver_df = silver_df \
    .withColumn("silver_processed_at", current_timestamp()) \
    .drop("sensor_mean", "sensor_stddev")  # internal columns, don't persist

# ---------------------------------------------------------------------------
# Step 8 — Write Silver Delta to MinIO
# ---------------------------------------------------------------------------

silver_df.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("facility_id", "date", "hour") \
    .save(SILVER_PATH)

print(f"Silver written to {SILVER_PATH}")

# Write VPD as separate Delta table
vpd_df.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("facility_id") \
    .save("s3a://greenhouse-lake/silver/vpd")

print(f"VPD written to s3a://greenhouse-lake/silver/vpd")
print("Silver job complete")

spark.stop()