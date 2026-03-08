# Flight Data Pipeline

An end-to-end data engineering pipeline that ingests real-time flight data over India from the OpenSky Network API, processes and transforms it using PySpark, and loads it into BigQuery for analytics. The pipeline is built on a modern open-source stack including Airflow for orchestration, Terraform for infrastructure provisioning, and Looker Studio for visualization. Designed to run fully on the GCP free tier with historical data accumulation and deduplication built in.

## Ingestion Architecture
```
OpenSky API
      ↓ fetch_flights.py
GCS — raw JSON (accumulates, never deleted)
      ↓ spark_-_transform.py
GCS — processed Parquet (overwritten per day)
      ↓ load_to_bigquery.py
BigQuery — flight_staging.raw_flights (MERGE with dedup)
      ↓ transform.py
BigQuery — flight_marts
              ├── fct_flights
              ├── dim_countries
              ├── dim_flight_phases
              └── agg_flights_daily
```