import random
import math
from datetime import datetime
from fastapi import FastAPI, HTTPException
import statistics

app = FastAPI(title="Greenhouse Sensor API", version="2.0.0")

# ---------------------------------------------------------------------------
# Facility & crop configuration
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# Sensor layout per facility
# Multiple sensors per type where spatial variance is meaningful.
# Airflow gets one — it's a zone-level actuator reading, not spatial.
# Each sensor has a small fixed positional bias (simulates placement effect).
# ---------------------------------------------------------------------------

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

# ---------------------------------------------------------------------------
# Anomaly injection
# ---------------------------------------------------------------------------

def maybe_anomaly(value, anomaly_rate=0.02):
    """
    Inject realistic anomalies at ~2% rate per sensor.
    Returns (value, anomaly_type | None).
    dropout returns None — Silver layer will interpolate or flag missing.
    """
    if value is None:
        return None, None
    if random.random() < anomaly_rate:
        anomaly_type = random.choice(["spike", "dropout", "drift"])
        if anomaly_type == "spike":
            return round(value * random.uniform(2.0, 3.0), 2), "spike"
        elif anomaly_type == "dropout":
            return None, "dropout"
        elif anomaly_type == "drift":
            return round(value + random.uniform(6, 10), 2), "drift"
    return value, None

# ---------------------------------------------------------------------------
# Peer-comparison anomaly detection
# Flags a sensor if its reading is > 2 std devs from the mean of its peers.
# Only meaningful when there are 2+ sensors for a type.
# ---------------------------------------------------------------------------

def flag_peer_outliers(readings: list[dict]) -> list[dict]:
    """
    Given a list of sensor readings for one sensor type in one facility,
    add is_peer_outlier=True to any reading that deviates significantly
    from the group mean. Skips dropout sensors (value=None).
    """
    valid_values = [r["value"] for r in readings if r["value"] is not None]

    if len(valid_values) < 2:
        for r in readings:
            r["is_peer_outlier"] = False
        return readings

    mean = statistics.mean(valid_values)
    stdev = statistics.stdev(valid_values) if len(valid_values) > 1 else 0

    for r in readings:
        if r["value"] is None:
            r["is_peer_outlier"] = False
        elif stdev > 0 and abs(r["value"] - mean) > 2 * stdev:
            r["is_peer_outlier"] = True
            r["peer_deviation"] = round(r["value"] - mean, 3)
        else:
            r["is_peer_outlier"] = False

    return readings

# ---------------------------------------------------------------------------
# Core reading generator
# ---------------------------------------------------------------------------

def generate_sensor_readings(facility_id: str, sensor_type: str, base_value: float) -> dict:
    """
    Generate readings for all sensors of one type in one facility.
    Returns a structured dict with per-sensor readings + facility aggregate.
    """
    layout = SENSOR_LAYOUT[facility_id][sensor_type]
    unit = SENSOR_UNITS[sensor_type]
    timestamp = datetime.utcnow().isoformat()

    raw_readings = []
    for sensor in layout:
        # base + positional bias + small random noise
        value = round(base_value + sensor["positional_bias"] + random.uniform(-0.5, 0.5), 2)
        value, anomaly_type = maybe_anomaly(value)

        reading = {
            "sensor_id": sensor["sensor_id"],
            "value": value,
            "unit": unit,
            "timestamp": timestamp,
            "anomaly_injected": anomaly_type  # None if clean — Silver will use this
        }
        raw_readings.append(reading)

    # peer comparison — adds is_peer_outlier to each reading
    raw_readings = flag_peer_outliers(raw_readings)

    # facility-level aggregate: mean of non-None, non-outlier values
    clean_values = [
        r["value"] for r in raw_readings
        if r["value"] is not None and not r["is_peer_outlier"]
    ]
    facility_aggregate = round(statistics.mean(clean_values), 2) if clean_values else None

    return {
        "sensors": raw_readings,
        "facility_aggregate": facility_aggregate,  # what Gold layer uses for climate score
        "sensor_count": len(raw_readings),
        "healthy_sensor_count": len(clean_values)
    }

# ---------------------------------------------------------------------------
# Full facility reading
# ---------------------------------------------------------------------------

def generate_facility_readings(facility_id: str) -> dict:
    facility = FACILITIES[facility_id]
    crop = facility["crop_type"]
    profile = CROP_PROFILES[crop]

    # Daily sinusoidal offset — temperature and light vary through the day
    hour = datetime.now().hour
    daily_temp_offset  = 2 * math.sin(math.pi * hour / 12)
    daily_light_offset = max(0, 100 * math.sin(math.pi * hour / 12))  # 0 at night

    base_values = {
        "temperature": round(random.uniform(*profile["temp_range"]) + daily_temp_offset, 2),
        "humidity":    round(random.uniform(*profile["humidity_range"]), 2),
        "co2":         round(random.uniform(*profile["co2_range"]), 2),
        "light":       round(random.uniform(*profile["light_range"]) + daily_light_offset, 2),
        "airflow":     round(random.uniform(*profile["airflow_range"]), 3)
    }

    readings = {
        sensor_type: generate_sensor_readings(facility_id, sensor_type, base)
        for sensor_type, base in base_values.items()
    }

    response = {
        "facility_id": facility_id,
        "crop_type": crop,
        "growth_week": facility["growth_week"],
        "location": facility["location"],
        "polled_at": datetime.utcnow().isoformat(),
        "readings": readings
    }

    # Irrigation fires ~10% of polls as a discrete event
    if random.random() < 0.10:
        response["irrigation_event"] = {
            "zone_id": f"{facility_id}_zone1",
            "started_at": datetime.utcnow().isoformat(),
            "duration_seconds": random.randint(60, 300),
            "volume_liters": round(random.uniform(10, 50), 2),
            "water_temp_celsius": round(random.uniform(16, 20), 2)
        }

    return response

# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

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

@app.get("/facilities/{facility_id}/readings/{sensor_type}")
def get_sensor_type_readings(facility_id: str, sensor_type: str):
    """Convenience endpoint: readings for one sensor type only."""
    if facility_id not in FACILITIES:
        raise HTTPException(status_code=404, detail=f"Facility '{facility_id}' not found")
    if sensor_type not in SENSOR_UNITS:
        raise HTTPException(status_code=404, detail=f"Sensor type '{sensor_type}' not found")

    facility = FACILITIES[facility_id]
    profile = CROP_PROFILES[facility["crop_type"]]
    hour = datetime.now().hour

    base_map = {
        "temperature": random.uniform(*profile["temp_range"]) + 2 * math.sin(math.pi * hour / 12),
        "humidity":    random.uniform(*profile["humidity_range"]),
        "co2":         random.uniform(*profile["co2_range"]),
        "light":       random.uniform(*profile["light_range"]),
        "airflow":     random.uniform(*profile["airflow_range"])
    }

    return {
        "facility_id": facility_id,
        "sensor_type": sensor_type,
        "polled_at": datetime.utcnow().isoformat(),
        **generate_sensor_readings(facility_id, sensor_type, base_map[sensor_type])
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)