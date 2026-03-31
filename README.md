# Greenhouse Data Platform

A production-grade IoT data streaming platform simulating smart greenhouse monitoring, built around the architecture of commercial controlled-environment agriculture systems.

---

## Architecture

```
Mock API (FastAPI)
    ↓
Ingestion Service → Kafka (temperature, humidity, CO2, light, airflow)
                         ↓                        ↓
              Consumer Service            Bronze Spark Job (streaming)
              (fast path)                 Parquet → MinIO every 30s
                    ↓                              ↓
           Postgres: live_readings        Airflow DAG (every 30 min)
                    ↓                      Silver → Gold → Postgres
                    └────────────────────────────┘
                                ↓
                            Grafana
```

Two data paths run in parallel. The fast path writes directly to Postgres on every Kafka message — Grafana reads this for live sensor gauges. The batch path lands raw Parquet in MinIO, which Airflow processes every 30 minutes through Silver and Gold Spark jobs, writing aggregated metrics back to Postgres for the trend panels.

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
| Containerisation | Docker Compose |

---

## Data Model

Bronze / Silver / Gold medallion architecture:

- **Bronze** — raw Parquet, partitioned by `facility_id / date / hour`, never modified after landing
- **Silver** — cleaned and enriched: anomaly detection (dropout, statistical 3-sigma, physiological range), VPD derived from paired temperature and humidity readings
- **Gold** — 30-minute aggregates: temperature, humidity, CO2, light, airflow, VPD, climate score, zone variance, anomaly rate

Two Postgres tables feed Grafana: `live_readings` (one row per sensor, constantly upserted) and `fct_30min_climate` (one row per facility per 30-minute window).

---

## Simulated Environment

Two facilities, two crops, polled every 10 seconds:

| Facility | Crop | Location |
|---|---|---|
| facility_A | Tomato | Amsterdam |
| facility_B | Cucumber | Rotterdam |

11 sensors per facility across 3 zones. Anomalies injected at ~2% rate (spike, dropout, drift). Peer outlier detection flags sensors deviating more than 2 standard deviations from zone neighbours.

---

## Derived Metrics

| Metric | Description |
|---|---|
| VPD | Vapour Pressure Deficit — key indicator of plant transpiration stress |
| Climate Score | 0–100, measures proximity to crop-specific optimal ranges |
| Zone Variance | Temperature std dev across zones — high values indicate uneven distribution |
| Anomaly Rate | Proportion of flagged readings per 30-minute window |

---

## Running Locally

Requires Docker Desktop with at least 8GB RAM allocated.

```bash
git clone https://github.com/vanjaduric/greenhouse-platform
cd greenhouse-platform
cp .env.example .env
# fill in .env with your credentials
docker-compose up --build
```

| Service | URL |
|---|---|
| Grafana | http://localhost:3000 |
| Airflow | http://localhost:8081 |
| MinIO | http://localhost:9001 |
| Mock API | http://localhost:8000/docs |

---

## Project Structure

```
greenhouse-platform/
├── mock_api/                   FastAPI sensor simulator
├── ingestion_service/          polls API, routes to Kafka topics
├── consumer_service/           Kafka → Postgres live_readings (fast path)
├── spark/
│   ├── bronze_job.py           Structured Streaming — Kafka → Parquet → MinIO
│   ├── silver_job.py           cleans, validates, calculates VPD
│   ├── gold_job.py             aggregates, scores, writes to MinIO + Postgres
│   └── Dockerfile
├── dags/
│   ├── greenhouse_pipeline.py
│   └── config.py
├── grafana/
│   ├── provisioning/
│   └── dashboards/
├── airflow.Dockerfile
├── docker-compose.yml
├── init-kafka-topics.sh
├── init-postgres.sql
└── .env.example
```
