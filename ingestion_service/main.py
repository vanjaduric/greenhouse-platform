import json
import time
import requests
from datetime import datetime
from kafka import KafkaProducer
import os

# Config
# -------------------------------------------------------------------------------------

API_BASE_URL = os.getenv("API_BASE_URL")
FACILITIES = ["facility_A", "facility_B"]
POLL_INTERVAL_SECONDS = 10
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")

TOPIC_MAP = {
    "temperature": "sensors.temperature",
    "humidity":    "sensors.humidity",
    "co2":         "sensors.co2",
    "light":       "sensors.light",
    "airflow":     "sensors.airflow"
}

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Kafka connected")


def send_to_dlq(message, reason, source):
    dlq_message = {
        "original_message": message,
        "failure_reason": reason,
        "source": source,
        "failed_at": datetime.utcnow().isoformat()
    }
    producer.send("dead.letter.queue", value=dlq_message)
    print(f"  ⚠ DLQ: {reason}")


def poll_facility(facility_id):
    print(f"  Polling {facility_id}...")
    url = f"{API_BASE_URL}/facilities/{facility_id}/readings"

    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()
        print(f"  Got data from API")
    except requests.exceptions.Timeout:
        send_to_dlq({"facility_id": facility_id}, "API timeout", url)
        return
    except requests.exceptions.ConnectionError:
        send_to_dlq({"facility_id": facility_id}, "API connection error", url)
        return
    except Exception as e:
        print(f"  API error: {e}")
        send_to_dlq({"facility_id": facility_id}, str(e), url)
        return

    crop_type   = data.get("crop_type")
    growth_week = data.get("growth_week")

    for sensor_key, topic in TOPIC_MAP.items():
        sensor_group = data.get("readings", {}).get(sensor_key)
        print(f"  Processing {sensor_key}: {sensor_group is not None}")

        if sensor_group is None:
            send_to_dlq(
                {"facility_id": facility_id, "sensor": sensor_key},
                f"missing sensor key: {sensor_key}",
                url
            )
            continue

        for reading in sensor_group["sensors"]:
            if reading.get("value") is None:
                send_to_dlq(reading, "null sensor value (dropout)", topic)
                continue

            reading["facility_id"]          = facility_id
            reading["crop_type"]            = crop_type
            reading["growth_week"]          = growth_week
            reading["ingested_at"]          = datetime.utcnow().isoformat()
            reading["facility_aggregate"]   = sensor_group["facility_aggregate"]
            reading["sensor_count"]         = sensor_group["sensor_count"]
            reading["healthy_sensor_count"] = sensor_group["healthy_sensor_count"]

            producer.send(topic, value=reading)

    irrigation = data.get("irrigation_event")
    if irrigation:
        irrigation["facility_id"] = facility_id
        irrigation["crop_type"]   = crop_type
        irrigation["ingested_at"] = datetime.utcnow().isoformat()
        producer.send("events.irrigation", value=irrigation)
        print(f"  💧 irrigation event routed for {facility_id}")

    producer.flush()
    print(f"[{datetime.utcnow().isoformat()}] Polled {facility_id} → all sensors routed")

# Looooooooooooooooooooooooooop

print(f"Ingestion service started. Polling every {POLL_INTERVAL_SECONDS}s")
print(f"Facilities: {FACILITIES}")
print("-" * 60)

while True:
    for facility_id in FACILITIES:
        poll_facility(facility_id)
    time.sleep(POLL_INTERVAL_SECONDS)