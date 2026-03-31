"""
Mock API: simulates greenhouse sensor readings for two facilities.
Generates realistic multi-sensor data with positional bias, daily
sinusoidal variation, and ~2% anomaly injection rate.
"""
import random
import math
from datetime import datetime
import statistics
from fastapi import FastAPI, HTTPException

app = FastAPI(title="Greenhouse Sensor API", version="2.0.0")

FACILITIES = {
    "facility_A": {
        "crop_type": "tomato",
        "location": "Amsterdam, NL",
        "growth_week": 6
    },
    "facility_B": {
        "crop_type": "cucumber",
        "location": "Rotterdam, NL",
        "growth_week": 4
    }
}

CROP_PROFILES = {
    "tomato": {
        "temp_range":     (20, 26),
        "humidity_range": (65, 75),
        "co2_range":      (800, 1100),
        "light_range":    (200, 600),
        "airflow_range":  (0.2, 0.8)
    },
    "cucumber": {
        "temp_range":     (22, 28),
        "humidity_range": (70, 80),
        "co2_range":      (700, 1000),
        "light_range":    (150, 500),
        "airflow_range":  (0.2, 0.7)
    }
}

SENSOR_LAYOUT = {
    "facility_A": {
        "temperature": [
            {"sensor_id": "temp_A_zone1", "positional_bias": -1.2},
            {"sensor_id": "temp_A_zone2", "positional_bias":  0.0},
            {"sensor_id": "temp_A_zone3", "positional_bias":  0.8},
        ],
        "humidity": [
            {"sensor_id": "hum_A_zone1", "positional_bias": -2.0},
            {"sensor_id": "hum_A_zone2", "positional_bias":  1.5},
        ],
        "co2": [
            {"sensor_id": "co2_A_zone1", "positional_bias": -30.0},
            {"sensor_id": "co2_A_zone2", "positional_bias":  20.0},
        ],
        "light": [
            {"sensor_id": "par_A_zone1", "positional_bias": -15.0},
            {"sensor_id": "par_A_zone2", "positional_bias":   5.0},
            {"sensor_id": "par_A_zone3", "positional_bias":  20.0},
        ],
        "airflow": [
            {"sensor_id": "air_A_zone1", "positional_bias": 0.0},
        ]
    },
    "facility_B": {
        "temperature": [
            {"sensor_id": "temp_B_zone1", "positional_bias": -0.8},
            {"sensor_id": "temp_B_zone2", "positional_bias":  0.0},
            {"sensor_id": "temp_B_zone3", "positional_bias":  1.1},
        ],
        "humidity": [
            {"sensor_id": "hum_B_zone1", "positional_bias": -1.0},
            {"sensor_id": "hum_B_zone2", "positional_bias":  2.0},
        ],
        "co2": [
            {"sensor_id": "co2_B_zone1", "positional_bias":  10.0},
            {"sensor_id": "co2_B_zone2", "positional_bias": -25.0},
        ],
        "light": [
            {"sensor_id": "par_B_zone1", "positional_bias": -10.0},
            {"sensor_id": "par_B_zone2", "positional_bias":  10.0},
            {"sensor_id": "par_B_zone3", "positional_bias":  25.0},
        ],
        "airflow": [
            {"sensor_id": "air_B_zone1", "positional_bias": 0.0},
        ]
    }
}

SENSOR_UNITS = {
    "temperature": "celsius",
    "humidity":    "percent",
    "co2":         "ppm",
    "light":       "umol_m2_s",
    "airflow":     "m_per_s"
}

def maybe_anomaly(value, anomaly_rate=0.02):
    if value is None:
        return None
    if random.random() < anomaly_rate:
        anomaly_type = random.choice(["spike", "dropout", "drift"])
        if anomaly_type == "spike":
            return round(value * random.uniform(2.0, 3.0), 2)
        elif anomaly_type == "dropout":
            return None
        elif anomaly_type == "drift":
            return round(value + random.uniform(6, 10), 2)
    return value

def flag_peer_outliers(readings):
    valid_values = [r["value"] for r in readings if r["value"] is not None]

    for r in readings:
        r["is_peer_outlier"] = False
        r["peer_deviation"] = None

    if len(valid_values) < 2:
        return readings

    mean_val = statistics.mean(valid_values)
    stdev = statistics.stdev(valid_values)

    for r in readings:
        if r["value"] is not None and stdev > 0 and abs(r["value"] - mean_val) > 2 * stdev:
            r["is_peer_outlier"] = True
            r["peer_deviation"] = round(r["value"] - mean_val, 3)

    return readings

def generate_sensor_readings(facility_id, sensor_type, base_value):
    layout = SENSOR_LAYOUT[facility_id][sensor_type]
    unit = SENSOR_UNITS[sensor_type]
    timestamp = datetime.utcnow().isoformat()

    raw_readings = []
    for sensor in layout:
        value = round(base_value + sensor["positional_bias"] + random.uniform(-0.5, 0.5), 2)
        value = maybe_anomaly(value)
        raw_readings.append({
            "sensor_id": sensor["sensor_id"],
            "value": value,
            "unit": unit,
            "timestamp": timestamp,
        })

    raw_readings = flag_peer_outliers(raw_readings)

    clean_values = [
        r["value"] for r in raw_readings
        if r["value"] is not None and not r["is_peer_outlier"]
    ]
    facility_aggregate = round(statistics.mean(clean_values), 2) if clean_values else None

    return {
        "sensors": raw_readings,
        "facility_aggregate": facility_aggregate,
        "sensor_count": len(raw_readings),
        "healthy_sensor_count": len(clean_values)
    }

def generate_facility_readings(facility_id):
    facility = FACILITIES[facility_id]
    crop = facility["crop_type"]
    profile = CROP_PROFILES[crop]

    hour = datetime.now().hour
    daily_temp_offset  = 2 * math.sin(math.pi * hour / 12)
    daily_light_offset = max(0, 100 * math.sin(math.pi * hour / 12))

    base_values = {
        "temperature": round(random.uniform(*profile["temp_range"]) + daily_temp_offset, 2),
        "humidity":    round(random.uniform(*profile["humidity_range"]), 2),
        "co2":         round(random.uniform(*profile["co2_range"]), 2),
        "light":       round(random.uniform(*profile["light_range"]) + daily_light_offset, 2),
        "airflow":     round(random.uniform(*profile["airflow_range"]), 3)
    }

    return {
        "facility_id":  facility_id,
        "crop_type":    crop,
        "growth_week":  facility["growth_week"],
        "location":     facility["location"],
        "polled_at":    datetime.utcnow().isoformat(),
        "readings": {
            sensor_type: generate_sensor_readings(facility_id, sensor_type, base)
            for sensor_type, base in base_values.items()
        }
    }

@app.get("/")
def root():
    return {"service": "Greenhouse Sensor API v2", "facilities": list(FACILITIES.keys())}

@app.get("/facilities")
def list_facilities():
    return {"facilities": FACILITIES}

@app.get("/facilities/{facility_id}/readings")
def get_readings(facility_id: str):
    if facility_id not in FACILITIES:
        raise HTTPException(status_code=404, detail=f"Facility '{facility_id}' not found")
    return generate_facility_readings(facility_id)