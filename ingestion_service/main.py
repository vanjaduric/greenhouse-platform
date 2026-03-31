"""
Ingestion service: polls the Mock API every 10 seconds and routes
sensor readings to the appropriate Kafka topics.
"""
import json
import time
import requests
from datetime import datetime
from kafka import KafkaProducer
import os

API_BASE_URL    = os.getenv("API_BASE_URL")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
FACILITIES      = ["facility_A", "facility_B"]
POLL_INTERVAL   = 10

TOPIC_MAP = {
    "temperature": "sensors.temperature",
    "humidity":    "sensors.humidity",
    "co2":         "sensors.co2",
    "light":       "sensors.light",
    "airflow":     "sensors.airflow"
}

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def poll_facility(facility_id):
    url = f"{API_BASE_URL}/facilities/{facility_id}/readings"
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"Failed to poll {facility_id}: {e}")
        return

    crop_type   = data.get("crop_type")
    growth_week = data.get("growth_week")

    for sensor_key, topic in TOPIC_MAP.items():
        sensor_group = data.get("readings", {}).get(sensor_key)
        if sensor_group is None:
            continue

        for reading in sensor_group["sensors"]:
            reading["facility_id"]          = facility_id
            reading["crop_type"]            = crop_type
            reading["growth_week"]          = growth_week
            reading["ingested_at"]          = datetime.utcnow().isoformat()
            reading["facility_aggregate"]   = sensor_group["facility_aggregate"]
            reading["sensor_count"]         = sensor_group["sensor_count"]
            reading["healthy_sensor_count"] = sensor_group["healthy_sensor_count"]
            producer.send(topic, value=reading)

    producer.flush()

while True:
    for facility_id in FACILITIES:
        poll_facility(facility_id)
    time.sleep(POLL_INTERVAL)