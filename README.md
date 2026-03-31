# Greenhouse Data Platform

A production-grade IoT data streaming platform built to simulate smart greenhouse monitoring. Designed around the architecture of commercial controlled-environment agriculture platforms.

**Live demo:** `http://your-server-ip:3000` (login: admin / admin)

---

## Architecture

```
Mock API (FastAPI)
    ↓
Ingestion Service → Kafka topics (temperature, humidity, CO2, light, airflow, irrigation)
                         ↓                          ↓
              Consumer Service              Bronze Spark Job (streaming)
              (fast path)                   writes Parquet → MinIO every 30s
                    ↓                                ↓
           Postgres: live_readings         Airflow DAG (every 30 min)
                    ↓                        Silver → Gold → Postgres
                    └──────────────────────────────┘
                                  ↓
                              Grafana
```

Two data paths run in parallel:

**Fast path** — the consumer service reads directly from Kafka and upserts to `live_readings` in Postgres on every message. Grafana queries this for live sensor gauges that update every ~10 seconds.

**Batch path** — the Bronze Spark streaming job writes raw Parquet files to MinIO partitioned by facility, date, and hour. Airflow triggers Silver and Gold Spark jobs every 30 minutes, calculating derived metrics and writing aggregated results to both MinIO (full history) and Postgres (for Grafana dashboards).

---

## Stack

| Layer | Technology |
|---|---|
| Ingestion | Apache Kafka (KRaft, no Zookeeper) |
| Stream processing | PySpark Structured Streaming |
| Orchestration | Apache Airflow (LocalExecutor) |
| Object storage | MinIO (S3-compatible) |
| Database | PostgreSQL 15 |
| Dashboards | Grafana |
| Containerization | Docker Compose |

---

## Data Model

**Medallion architecture (Bronze / Silver / Gold):**

- **Bronze** — raw sensor readings as Parquet, partitioned by `facility_id / date / hour`. Never modified after landing.
- **Silver** — cleaned and enriched: type casting, anomaly flagging (statistical outliers, dropouts, range violations), VPD calculation from paired temperature and humidity readings.
- **Gold** — 30-minute aggregates per facility: average temperature, humidity, CO2, light, airflow, VPD, climate score, zone variance, anomaly rate.

**Postgres tables:**

- `live_readings` — one row per sensor (upserted), always reflects current state. Powers live Grafana gauges.
- `fct_30min_climate` — one row per facility per 30-minute window. Powers trend charts and derived metric panels.

---

## Simulated Environment

Two facilities running in parallel:

| Facility | Crop | Location |
|---|---|---|
| facility_A | Tomato | Amsterdam |
| facility_B | Cucumber | Rotterdam |

Each facility has 11 sensors across 3 zones: 3 temperature, 2 humidity, 2 CO2, 3 PAR light, 1 airflow.

Anomalies are injected at ~2% rate (spike, dropout, drift) to simulate real sensor failures. Peer outlier detection flags sensors that deviate more than 2 standard deviations from zone neighbours.

---

## Derived Metrics

| Metric | Description |
|---|---|
| VPD | Vapour Pressure Deficit — calculated from temperature + humidity. Key indicator of plant transpiration stress. |
| Climate Score | 0–100 score measuring how close each parameter was to crop-specific optimal ranges over the window. |
| Zone Variance | Standard deviation of temperature across zones — high variance indicates uneven climate distribution. |
| Anomaly Rate | Proportion of flagged readings in each 30-minute window across all sensor types. |

---

## Grafana Dashboard

**Row 1 — Live status (gauges):** current temperature, humidity, CO2 for both facilities. Updates every ~10 seconds from `live_readings`.

**Row 2 — Secondary live readings (stat panels):** PAR light and airflow per facility.

**Row 3-4 — Trend panels (time series):** VPD, climate score, anomaly rate, zone temperature variance over the last 24 hours. Updates every 30 minutes from `fct_30min_climate`.

**Row 5 — Anomaly table:** all sensors currently flagged as anomalous with facility, sensor ID, type, value, and timestamp.

---

## Running Locally

**Prerequisites:** Docker Desktop, at least 8GB RAM available to Docker.

```bash
git clone https://github.com/vanjadjuric/greenhouse-platform
cd greenhouse-platform

cp .env.example .env
# edit .env with your own credentials

docker-compose up --build
```

Services start in dependency order. Allow ~2 minutes for all health checks to pass.

| Service | URL |
|---|---|
| Grafana | http://localhost:3000 (admin / admin) |
| Airflow | http://localhost:8081 (airflow / airflow) |
| MinIO | http://localhost:9001 |
| Mock API | http://localhost:8000/docs |

---

## Project Structure

```
greenhouse-platform/
├── mock_api/               FastAPI sensor simulator
├── ingestion_service/      Kafka producer — polls API, routes to topics
├── consumer_service/       Kafka consumer — fast path to Postgres
├── spark/
│   ├── bronze_job.py       Structured Streaming — Kafka → Parquet → MinIO
│   ├── silver_job.py       Batch — clean, validate, calculate VPD
│   ├── gold_job.py         Batch — aggregate, climate score, write to Postgres
│   └── Dockerfile
├── dags/
│   └── greenhouse_pipeline.py   Airflow DAG — 30 min batch pipeline
├── grafana/
│   ├── provisioning/       Auto-provisioned datasource + dashboard
│   └── dashboards/         greenhouse.json
├── airflow.Dockerfile
├── docker-compose.yml
├── init-kafka-topics.sh
├── init-postgres.sql
└── .env.example
```

---

## Production Deployment

Deployed on Hetzner CAX11 (ARM, Ubuntu 24.04).

```bash
ssh root@your-server-ip
git clone https://github.com/vanjadjuric/greenhouse-platform
cd greenhouse-platform
cp .env.example .env && nano .env
docker compose up -d
```

---

