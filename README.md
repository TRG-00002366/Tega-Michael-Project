# TaxiOps: Real-Time NYC Yellow Taxi Trip Analytics Pipeline

## Overview
TaxiOps is an end-to-end **data engineering pipeline** that simulates NYC Yellow Taxi trip events using **Faker**, ingests them in real time with **Kafka**, processes and enriches them with **PySpark (Structured Streaming)**, and writes curated outputs to a local **Parquet-based data lake** using a **Bronze → Silver → Gold** (medallion) architecture. The workflow can be orchestrated and scheduled with **Apache Airflow**.

This project is designed to mirror production data platforms where:
- raw events are landed with minimal change (**Bronze**),
- cleaned/enriched datasets power downstream analytics (**Silver**),
- business-ready KPIs and aggregates are published for reporting (**Gold**).

---

## Business Scenario
NYC TLC stakeholders want near real-time visibility into taxi activity to understand:
- **Trip volume trends** by time of day (hourly patterns)
- **Revenue performance** by pickup zone and payment type
- **Trip efficiency** (duration, fare-per-mile, distance distribution)
- **Tip behavior** (tip rate distribution and averages)

---

## Architecture

```
┌──────────────────────────┐       ┌──────────────────────────┐       ┌─────────────────────────────┐
│  Trip Event Simulator     │       │                          │       │  PySpark Streaming Jobs      │
│  (Faker Producer)         │──────▶│   Kafka                  │──────▶│  Bronze Ingest / Silver ETL  │
│  (Topic: nyc_taxi_trips)  │       │  (Topic: nyc_taxi_trips) │       │  (Structured Streaming)      │
└──────────────────────────┘       └──────────────────────────┘       └───────────────┬─────────────┘
                                                                                      │
                                                                                      ▼
                                                                           ┌──────────────────────────┐
                                                                           │  Bronze Layer (Raw)      │
                                                                           │  Parquet (partitioned)   │
                                                                           └───────────────┬──────────┘
                                                                                           │
                                                                                           ▼
                                                                           ┌──────────────────────────┐
                                                                           │  Silver Layer (Clean)    │
                                                                           │  Parquet (enriched)      │
                                                                           └───────────────┬──────────┘
                                                                                           │
                                                                                           ▼
                                                                           ┌──────────────────────────┐
                                                                           │  Gold Layer (KPIs)       │
                                                                           │  Parquet (aggregated)    │
                                                                           └───────────────┬──────────┘
                                                                                           │
                                                                                           ▼
                                                                           ┌──────────────────────────┐
                                                                           │  Airflow DAG             │
                                                                           │  Orchestration (@daily)  │
                                                                           └──────────────────────────┘
```

---

## Tech Stack

| Technology | Purpose |
|------------|---------|
| PySpark (Structured Streaming) | Stream processing & ETL |
| Spark SQL / DataFrame API | Aggregations, joins, windows |
| Apache Kafka | Real-time ingestion |
| Faker (PyPI) | Real-time trip event generation |
| Apache Airflow | Orchestration & scheduling |
| Parquet | Columnar lake storage |

---

## Project Structure

```
Tega-Michael-Project-main/
├── README.md
├── Project_1_specs.md
├── docker-compose.yml
├── pyproject.toml
├── poetry.lock
└── src/
    ├── producer/
    │   └── faker_producer.py
    └── streaming/
        ├── bronze_ingest.py
        ├── silver_transform.py
        └── gold_aggregate.py
```

> **Note:** Current repo snapshot

---

## Data Model (Trip Event Schema)
Events produced to Kafka are JSON records modeled after TLC-style trip attributes.

Example:
```json
{
  "event_id": "a3f9c2d1-7b4e-4c1a-9f2a-6b8e91d2f001",
  "vendor_id": 2,
  "pickup_datetime": "2023-01-01T00:25:04Z",
  "dropoff_datetime": "2023-01-01T00:37:49Z",
  "passenger_count": 1,
  "trip_distance": 2.51,
  "pu_location_id": 48,
  "do_location_id": 238,
  "payment_type": 1,
  "fare_amount": 14.9,
  "tip_amount": 2.0,
  "total_amount": 20.4,
  "event_timestamp": "2026-03-03T15:12:00Z"
}
```

---

## Setup & Run Guide

### 1) Prerequisites
- Docker + Docker Compose
- Python 3.11 (per `pyproject.toml`)
- Java 8/11/17 (Spark-compatible)
- Spark 3.5.x installed (or available via `spark-submit`)

### 2) Start Kafka (Docker)
From the project root:
```bash
docker compose up -d
```

Confirm containers:
```bash
docker ps
```

### 3) Create Kafka Topic
```bash
docker exec -it $(docker ps -qf "name=kafka") \
  kafka-topics --create --topic nyc_taxi_trips --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1
```

List topics:
```bash
docker exec -it $(docker ps -qf "name=kafka") \
  kafka-topics --list --bootstrap-server localhost:9092
```

### 4) Install Python Dependencies
Using Poetry (recommended):
```bash
pip install poetry
poetry install
```

Or pip:
```bash
pip install faker kafka-python pyspark==3.5.8
```

### 5) Run the Faker Producer (Kafka)
Produces a trip event every second:
```bash
python src/producer/faker_producer.py
```

---

## Pipeline Modules & Expected Outputs

### Module 1 — Real-Time Data Generation (Faker Producer)
**File:** `src/producer/faker_producer.py`

**Goal:** continuously publish synthetic taxi trip events to Kafka.

**Expected behavior:**
- Topic: `nyc_taxi_trips`
- JSON payloads with a consistent schema
- Controlled throughput (e.g., 1 msg/sec) to simulate real-time ingestion

---

### Module 2 — Bronze Ingest (Raw Landing)
**File:** `src/streaming/bronze_ingest.py`

**Goal:** consume Kafka events with Spark Structured Streaming and land them as-is (raw) into the Bronze layer.

**Expected behavior:**
- Read from Kafka topic `nyc_taxi_trips`
- Parse JSON into a Spark schema
- Write raw events to Parquet
- Partition by `pickup_date` (derived from `pickup_datetime`)
- Micro-batch trigger (e.g., 1 minute)

**Example output paths:**
```
data/bronze/pickup_date=2023-01-01/part-*.parquet
```

---

### Module 3 — Silver Transform (Clean + Enrich)
**File:** `src/streaming/silver_transform.py`

**Goal:** clean and enrich Bronze events into an analytics-friendly Silver dataset.

**Common transformations:**
- Data quality checks (null/negative fare, invalid timestamps, etc.)
- Derived fields:
  - `trip_duration_min`
  - `pickup_date`, `pickup_hour`
  - `tip_rate` (tip / fare)
  - `fare_per_mile`
  - `trip_category` (short/medium/long)

**Example output:**
```
data/silver/pickup_date=2023-01-01/part-*.parquet
```

---

### Module 4 — Gold Aggregate (Business KPIs)
**File:** `src/streaming/gold_aggregate.py`

**Goal:** publish business-ready KPI tables from Silver.

**Recommended Gold tables:**
1. **Hourly Trips & Revenue**
   - group by `pickup_date`, `pickup_hour`
   - metrics: `total_trips`, `total_revenue`, `avg_fare`, `avg_tip_rate`

2. **Pickup Zone Revenue**
   - group by `pickup_date`, `pu_location_id`
   - metrics: `trips`, `revenue`, `avg_distance`

3. **Payment Type Breakdown**
   - group by `pickup_date`, `payment_type`
   - metrics: `trips`, `revenue`, `avg_fare`

**Storage best practices:**
- Write to Parquet
- Partition by `pickup_date`
- Use deterministic overwrite (or append + partition pruning)

**Example output:**
```
data/gold/hourly_kpis/pickup_date=2023-01-01/part-*.parquet
data/gold/zone_revenue/pickup_date=2023-01-01/part-*.parquet
data/gold/payment_kpis/pickup_date=2023-01-01/part-*.parquet
```

---

## Airflow Orchestration (Optional / Week 4)
If you add Airflow orchestration, the DAG typically:
- verifies Kafka is reachable and topic exists
- starts/monitors streaming ingestion (or ensures it’s running)
- runs daily Gold rollups for the previous execution date
- validates output existence and schema

**Recommended DAG shape:**
```
start >> check_kafka >> run_bronze >> run_silver >> run_gold >> validate_outputs >> end
```

---

## How to Validate Outputs
Examples:
- Check partitions created:
```bash
ls -R data/bronze | head
ls -R data/silver | head
ls -R data/gold | head
```

- Quick Spark read validation:
```bash
spark-shell
# or pyspark
```

---

## Team
- Michael Chen
- Tega Omarejedje

---

## Notes
- The NYC Taxi & Limousine Commission (TLC) source Parquet file can be used as a reference for schema realism, but the real-time pipeline uses Faker to generate streaming-like events.
- If you adopt the official TLC parquet (`yellow_tripdata_2023-01.parquet`), place it under:
  ```
  data/source/yellow_tripdata_2023-01.parquet
  ```

## Dataset Source

The dataset is published by the NYC Taxi & Limousine Commission (TLC).

Download the official trip record data here:

[NYC TLC Trip Record Data Portal](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)