import os
import json
import time
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
POSTGRES_CONN = os.getenv(
    "POSTGRES_CONN",
    f"host={os.getenv('POSTGRES_HOST')} "
    f"port=5432 "
    f"dbname={os.getenv('POSTGRES_DB')} "
    f"user={os.getenv('POSTGRES_USER')} "
    f"password={os.getenv('POSTGRES_PASS')}"
)
SENSOR_TOPICS = [
    "sensors.temperature",
    "sensors.humidity",
    "sensors.co2",
    "sensors.light",
    "sensors.airflow"
]

TOPIC_TO_TYPE = {
    "sensors.temperature": "temperature",
    "sensors.humidity":    "humidity",
    "sensors.co2":         "co2",
    "sensors.light":       "light",
    "sensors.airflow":     "airflow"
}

consumer = KafkaConsumer(
    *SENSOR_TOPICS,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    group_id="fast-path-consumer",
    auto_offset_reset="latest"  # only read new messages, not history
)

def get_conn():
    return psycopg2.connect(POSTGRES_CONN)

def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS live_readings (
                facility_id  VARCHAR NOT NULL,
                sensor_id    VARCHAR NOT NULL,
                sensor_type  VARCHAR NOT NULL,
                value        FLOAT,
                unit         VARCHAR,
                is_anomaly   BOOLEAN DEFAULT FALSE,
                recorded_at  TIMESTAMP,
                PRIMARY KEY (facility_id, sensor_id)
            )
        """)
    conn.commit()

def upsert_reading(conn, topic, msg):
    sensor_type = TOPIC_TO_TYPE.get(topic)
    if not sensor_type:
        return

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO live_readings 
                (facility_id, sensor_id, sensor_type, value, unit, is_anomaly, recorded_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (facility_id, sensor_id) 
            DO UPDATE SET
                value       = EXCLUDED.value,
                unit        = EXCLUDED.unit,
                is_anomaly  = EXCLUDED.is_anomaly,
                recorded_at = EXCLUDED.recorded_at
        """, (
            msg.get("facility_id"),
            msg.get("sensor_id"),
            sensor_type,
            msg.get("value"),
            msg.get("unit"),
            msg.get("anomaly_injected") is not None,
            msg.get("timestamp")
        ))
    conn.commit()

print("Fast path consumer started")
conn = get_conn()
ensure_table(conn)
print("live_readings table ready")
print("Consuming from Kafka → writing to Postgres...")

for message in consumer:
    try:
        upsert_reading(conn, message.topic, message.value)
    except Exception as e:
        print(f"Error: {e}")
        conn = get_conn()  # reconnect on error