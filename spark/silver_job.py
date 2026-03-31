"""
Silver Layer: cleaned, validated, enriched
Reads Bronze parquet for the current execution window only.
Triggered by Airflow every 30 minutes.
"""
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, isnan, isnull, stddev, mean,
    abs as spark_abs, current_timestamp, exp,
    round as spark_round, to_timestamp, unix_timestamp, lit
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BRONZE_PATH      = os.getenv("BRONZE_PATH")
SILVER_PATH      = os.getenv("SILVER_PATH")
EXECUTION_DATE   = os.getenv("EXECUTION_DATE")

exec_dt   = datetime.fromisoformat(EXECUTION_DATE)
exec_date = exec_dt.strftime("%Y-%m-%d")
exec_hour = exec_dt.hour
print(f"Silver job started — processing window: {exec_date} hour={exec_hour}")

spark = SparkSession.builder \
    .appName("greenhouse-silver") \
    .config("spark.jars.packages",
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

bronze_df_A = spark.read.parquet(
    f"{BRONZE_PATH}/facility_id=facility_A/date={exec_date}/hour={exec_hour}"
).withColumn("facility_id", lit("facility_A")) \
 .withColumn("date", lit(exec_date).cast("date")) \
 .withColumn("hour", lit(exec_hour))

bronze_df_B = spark.read.parquet(
    f"{BRONZE_PATH}/facility_id=facility_B/date={exec_date}/hour={exec_hour}"
).withColumn("facility_id", lit("facility_B")) \
 .withColumn("date", lit(exec_date).cast("date")) \
 .withColumn("hour", lit(exec_hour))

bronze_df = bronze_df_A.union(bronze_df_B)

silver_df = bronze_df \
    .withColumn("value",       col("value").cast(DoubleType())) \
    .withColumn("timestamp",   to_timestamp(col("timestamp"))) \
    .withColumn("ingested_at", to_timestamp(col("ingested_at")))

# Add window stats for statistical anomaly detection
sensor_window = Window.partitionBy("facility_id", "sensor_id")

silver_df = silver_df \
    .withColumn("sensor_mean",   mean(col("value")).over(sensor_window)) \
    .withColumn("sensor_stddev", stddev(col("value")).over(sensor_window))

silver_df = silver_df.withColumn(
    "anomaly_type",
    when(isnull(col("value")) | isnan(col("value")), "dropout")
    .when(
        col("sensor_stddev").isNotNull() &
        (spark_abs(col("value") - col("sensor_mean")) > 3 * col("sensor_stddev")),
        "statistical"
    )
    .when(
        ((col("source_topic") == "sensors.temperature") & ((col("value") < 5)   | (col("value") > 50)))   |
        ((col("source_topic") == "sensors.humidity")    & ((col("value") < 0)   | (col("value") > 100)))  |
        ((col("source_topic") == "sensors.co2")         & ((col("value") < 200) | (col("value") > 5000))) |
        ((col("source_topic") == "sensors.light")       & ((col("value") < 0)   | (col("value") > 2000))) |
        ((col("source_topic") == "sensors.airflow")     & ((col("value") < 0)   | (col("value") > 5))),
        "range"
    )
    .otherwise(None)
).withColumn(
    "is_anomaly",
    col("anomaly_type").isNotNull()
).withColumn(
    "silver_processed_at", current_timestamp()
).drop("sensor_mean", "sensor_stddev")

# VPD = (1 - RH/100) * 0.6108 * e^(17.27 * T / (T + 237.3))
temp_df = silver_df \
    .filter((col("source_topic") == "sensors.temperature") & ~col("is_anomaly")) \
    .select(col("facility_id"), col("timestamp"), col("value").alias("temperature"))

hum_df = silver_df \
    .filter((col("source_topic") == "sensors.humidity") & ~col("is_anomaly")) \
    .select(col("facility_id"), col("timestamp"), col("value").alias("humidity"))

vpd_df = temp_df.alias("t").join(
    hum_df.alias("h"),
    (col("t.facility_id") == col("h.facility_id")) &
    (spark_abs(unix_timestamp(col("t.timestamp")) - unix_timestamp(col("h.timestamp"))) <= 60),
    "inner"
).select(
    col("t.facility_id"),
    col("t.timestamp"),
    col("t.temperature"),
    col("h.humidity"),
    spark_round(
        (1 - col("h.humidity") / 100) *
        0.6108 *
        exp(17.27 * col("t.temperature") / (col("t.temperature") + 237.3)),
        4
    ).alias("vpd_kpa")
)

silver_df.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("facility_id", "date", "hour") \
    .save(SILVER_PATH)

vpd_df.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("facility_id") \
    .save(os.getenv("VPD_PATH"))

spark.stop()