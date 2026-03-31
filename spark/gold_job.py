"""
Gold layer: aggregated business metrics.
Reads Silver Delta for the current execution window only.
Triggered by Airflow every 30 minutes, after Silver completes.
"""
import os
import psycopg2
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, stddev, count, sum as spark_sum,
    when, window, round as spark_round,
    current_timestamp, lit, first
)
from pyspark.sql.types import FloatType

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
SILVER_PATH      = os.getenv("SILVER_PATH")
VPD_PATH         = os.getenv("VPD_PATH")
GOLD_PATH        = os.getenv("GOLD_PATH")
POSTGRES_HOST    = os.getenv("POSTGRES_HOST")
POSTGRES_DB      = os.getenv("POSTGRES_DB")
POSTGRES_USER    = os.getenv("POSTGRES_USER")
POSTGRES_PASS    = os.getenv("POSTGRES_PASS")
EXECUTION_DATE   = os.getenv("EXECUTION_DATE")

WINDOW_MINUTES = 30

exec_dt   = datetime.fromisoformat(EXECUTION_DATE)
exec_date = exec_dt.strftime("%Y-%m-%d")
exec_hour = exec_dt.hour

print(f"Gold job started — processing window: {exec_date} hour={exec_hour}")

spark = SparkSession.builder \
    .appName("greenhouse-gold") \
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.1.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "org.postgresql:postgresql:42.6.0") \
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

silver_df_A = spark.read.format("delta").load(
    f"{SILVER_PATH}/facility_id=facility_A/date={exec_date}/hour={exec_hour}"
).withColumn("facility_id", lit("facility_A"))

silver_df_B = spark.read.format("delta").load(
    f"{SILVER_PATH}/facility_id=facility_B/date={exec_date}/hour={exec_hour}"
).withColumn("facility_id", lit("facility_B"))

silver_df = silver_df_A.union(silver_df_B)
vpd_df    = spark.read.format("delta").load(VPD_PATH)

agg_df = silver_df \
    .filter(~col("is_anomaly")) \
    .groupBy(
        col("facility_id"),
        col("crop_type"),
        col("growth_week"),
        window(col("timestamp"), f"{WINDOW_MINUTES} minutes").alias("time_window"),
        col("source_topic")
    ).agg(
        spark_round(avg("value"), 3).alias("avg_value"),
        spark_round(stddev("value"), 3).alias("zone_variance"),
        count("value").alias("reading_count"),
        spark_round(spark_sum(
            when(col("is_anomaly"), 1).otherwise(0)
        ).cast(FloatType()) / count("value"), 4).alias("anomaly_rate")
    ) \
    .withColumn("window_start", col("time_window.start")) \
    .drop("time_window")

pivoted_df = agg_df.groupBy(
    "facility_id", "crop_type", "growth_week", "window_start"
).pivot("source_topic", [
    "sensors.temperature",
    "sensors.humidity",
    "sensors.co2",
    "sensors.light",
    "sensors.airflow"
]).agg(
    first("avg_value").alias("avg"),
    first("zone_variance").alias("variance"),
    first("anomaly_rate").alias("anomaly_rate")
)

pivoted_df = pivoted_df \
    .withColumnRenamed("sensors.temperature_avg",          "avg_temperature") \
    .withColumnRenamed("sensors.humidity_avg",             "avg_humidity") \
    .withColumnRenamed("sensors.co2_avg",                  "avg_co2") \
    .withColumnRenamed("sensors.light_avg",                "avg_light") \
    .withColumnRenamed("sensors.airflow_avg",              "avg_airflow") \
    .withColumnRenamed("sensors.temperature_variance",     "variance_temperature") \
    .withColumnRenamed("sensors.temperature_anomaly_rate", "anomaly_rate_temperature") \
    .withColumnRenamed("sensors.humidity_anomaly_rate",    "anomaly_rate_humidity") \
    .withColumnRenamed("sensors.co2_anomaly_rate",         "anomaly_rate_co2")

vpd_windowed = vpd_df.groupBy(
    col("facility_id"),
    window(col("timestamp"), f"{WINDOW_MINUTES} minutes").alias("time_window")
).agg(
    spark_round(avg("vpd_kpa"), 4).alias("avg_vpd")
).withColumn("window_start", col("time_window.start")).drop("time_window")

gold_df = pivoted_df.join(vpd_windowed, ["facility_id", "window_start"], "left")

def within_range_score(value_col, low, high):
    range_size = high - low
    return when(value_col.isNull(), None) \
        .when((value_col >= low) & (value_col <= high), lit(100.0)) \
        .when(value_col < low,
              spark_round(
                  (100 - (lit(low) - value_col) / range_size * 100).cast(FloatType()), 2
              )) \
        .otherwise(
            spark_round(
                (100 - (value_col - lit(high)) / range_size * 100).cast(FloatType()), 2
            )
        )

gold_df = gold_df \
    .withColumn("score_temperature", within_range_score(col("avg_temperature"), 20, 26)) \
    .withColumn("score_humidity",    within_range_score(col("avg_humidity"),    65, 75)) \
    .withColumn("score_co2",         within_range_score(col("avg_co2"),         800, 1100)) \
    .withColumn("score_light",       within_range_score(col("avg_light"),       200, 600)) \
    .withColumn("climate_score",
        spark_round(
            (col("score_temperature") +
             col("score_humidity") +
             col("score_co2") +
             col("score_light")) / 4, 2
        )
    ) \
    .withColumn("overall_anomaly_rate",
        spark_round(
            (col("anomaly_rate_temperature") +
             col("anomaly_rate_humidity") +
             col("anomaly_rate_co2")) / 3 * 100, 4
        )
    ) \
    .withColumn("gold_processed_at", current_timestamp())

gold_df.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("facility_id") \
    .save(f"{GOLD_PATH}/fct_30min_climate")

def ensure_gold_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fct_30min_climate (
                facility_id          VARCHAR NOT NULL,
                crop_type            VARCHAR,
                growth_week          INTEGER,
                window_start         TIMESTAMP NOT NULL,
                avg_temperature      FLOAT,
                avg_humidity         FLOAT,
                avg_co2              FLOAT,
                avg_light            FLOAT,
                avg_airflow          FLOAT,
                avg_vpd              FLOAT,
                climate_score        FLOAT,
                variance_temperature FLOAT,
                overall_anomaly_rate FLOAT,
                gold_processed_at    TIMESTAMP,
                PRIMARY KEY (facility_id, window_start)
            )
        """)
    conn.commit()

def write_row_to_postgres(conn, row):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO fct_30min_climate (
                facility_id, crop_type, growth_week, window_start,
                avg_temperature, avg_humidity, avg_co2, avg_light, avg_airflow,
                avg_vpd, climate_score, variance_temperature,
                overall_anomaly_rate, gold_processed_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (facility_id, window_start)
            DO UPDATE SET
                avg_temperature      = EXCLUDED.avg_temperature,
                avg_humidity         = EXCLUDED.avg_humidity,
                avg_co2              = EXCLUDED.avg_co2,
                avg_light            = EXCLUDED.avg_light,
                avg_airflow          = EXCLUDED.avg_airflow,
                avg_vpd              = EXCLUDED.avg_vpd,
                climate_score        = EXCLUDED.climate_score,
                variance_temperature = EXCLUDED.variance_temperature,
                overall_anomaly_rate = EXCLUDED.overall_anomaly_rate,
                gold_processed_at    = EXCLUDED.gold_processed_at
        """, (
            row.facility_id, row.crop_type, row.growth_week, row.window_start,
            row.avg_temperature, row.avg_humidity, row.avg_co2,
            row.avg_light, row.avg_airflow, row.avg_vpd,
            row.climate_score, row.variance_temperature,
            row.overall_anomaly_rate, row.gold_processed_at
        ))
    conn.commit()

pg_conn = psycopg2.connect(
    host=POSTGRES_HOST, dbname=POSTGRES_DB,
    user=POSTGRES_USER, password=POSTGRES_PASS
)

ensure_gold_table(pg_conn)

gold_rows = gold_df.select(
    "facility_id", "crop_type", "growth_week", "window_start",
    "avg_temperature", "avg_humidity", "avg_co2", "avg_light", "avg_airflow",
    "avg_vpd", "climate_score", "variance_temperature",
    "overall_anomaly_rate", "gold_processed_at"
).collect()

for row in gold_rows:
    write_row_to_postgres(pg_conn, row)

pg_conn.close()
spark.stop()