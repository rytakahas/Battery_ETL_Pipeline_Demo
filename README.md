## Battery Degradation Analytics Pipeline

This project builds a **modern streaming data pipeline** to ingest, store, clean, and transform **battery degradation data** using:

- **Redpanda (Kafka)** for real-time streaming
- **MinIO** for data lake
- **MotherDuck** as a scalable DuckDB-based analytical warehouse
- **dbt** for data transformations
- **Airflow (Astro)** for orchestration and scheduling

---

###  Architecture Overview

```plaintext
[Redpanda Kafka]  →  [Python Producer (Streaming)]  
     ↓
[Raw Table in MinIO (battery_ts_cleaned)]
     ↓
[dbt Models] → battery_ts (view), battery_cleaned (table), battery_features (table)
     ↓
[Clean Table in MotherDuck (battery_ts)]
     ↓
[Airflow DAG (Astro)] → Orchestrates:
    • Kafka streaming checks
    • Data cleaning and transformation
    • dbt model refreshes and validation
    • Logging and alerts
```

### Project Structure
```bash
├── Dockerfile
├── README.md
├── airflow/
│   └── dags/
│       └── battery_pipeline.py
├── dags/
│   ├── battery_pipeline.py
│   └── exampledag.py
├── dbt_project/
│   ├── dbt_project.yml
│   └── models/
│       ├── battery_cleaned.sql
│       ├── battery_features.sql
│       ├── battery_ts.sql
│       └── schema.yml
├── docker-compose.yml
├── include/
│   └── .dbt/
│       └── profiles.yml
└── notebook/
    └── ingest_battery_stream.ipynb

.env                            # Stores MOTHERDUCK_TOKEN                     
```

### Setup Instructions
1. Clone Repository
```bash
git clone https://github.com/your-org/battery-pipeline
cd battery-pipeline
```
2. Create .env File
```env
MOTHERDUCK_TOKEN=your_motherduck_token_here
MINIO_ENDPOINT=your_minio_endpoint
MINIO_ACCESS_KEY=your_minio_access_key
MINIO_SECRET_KEY=your_minio_secret_key
MINIO_BUCKET=battery-data
```

3. Start Astro Dev Environment
```bash
astro dev start
```

4. Run Redpanda (Kafka) in Docker
```bash
docker run -d --name redpanda \
  -p 9092:9092 -p 9644:9644 \
  docker.redpanda.com/redpandadata/redpanda:v23.3.10 \
  start --overprovisioned --smp 1 --memory 1G --reserve-memory 0M \
  --node-id 0 --check=false
```

To verify:

```bash
docker exec -it redpanda rpk topic list
```

### dbt Models
Model	Type	Description
battery_ts.sql	view	Thin view of battery_ts_cleaned
battery_cleaned	table	Cleans nulls, parses numerics, drops outliers
battery_features	table	Adds power_watt, total_capacity columns

Edit materialization in dbt_project.yml if needed.

### Airflow DAG (Astro)
File: airflow/dags/battery_pipeline.py

Runs every hour and:

Checks if new streamed data exists

Cleans and stores data as battery_cleaned

Overwrites battery_ts for analytics

Runs dbt run and dbt test

### Query Examples in MotherDuck
```sql
-- Count rows by source file
SELECT source_file, COUNT(*) FROM battery_ts GROUP BY source_file;

-- Check max voltage over time
SELECT MAX("voltage(v)"), streamed_at FROM battery_ts GROUP BY streamed_at ORDER BY streamed_at DESC;

-- Feature: Power = Voltage * Current
SELECT
  *,
  CAST("voltage(v)" AS DOUBLE) * CAST("current(a)" AS DOUBLE) AS power_watt,
  CAST("charge_capacity(ah)" AS DOUBLE) + CAST("discharge_capacity(ah)" AS DOUBLE) AS total_capacity
FROM {{ ref('battery_cleaned') }}
```

### Requirements
Python 3.9+

Docker

Astro CLI

dbt-duckdb, DuckDB, Redpanda, boto3

MotherDuck account (token-based access)

Optional: AWS S3-compatible endpoint (MinIO, iDrive, etc.)

### Testing
You can run individual steps in the Astro UI, or test locally using:

bash
Copy
Edit
astro dev run airflow tasks test battery_pipeline clean_battery_data <timestamp>
### Notes
Your MotherDuck token should be injected as an environment variable or copied to the container .dbt/profiles.yml.

Airflow logs and dbt output will help debug errors like profile misconfig or missing tables.

## Acknowledgements
CALCE Battery Dataset

MotherDuck + DuckDB

Astro by Astronomer

Redpanda for streaming Kafka replacement

## Future Work

As the volume, velocity, and variety of battery degradation data increase—particularly for real-time, multi-source IoT streams—this pipeline can be extended in several directions inspired by production-scale architectures used by companies like **Netflix** and **Amazon**:

### Scale for High-Throughput Workloads

1. **Apache Spark or PySpark Integration**  
   - For distributed transformation, cleaning, and feature engineering on large-scale time series datasets.
   - Enables in-memory aggregation, fault tolerance, and GPU acceleration.

2. **Apache Iceberg or Delta Lake**  
   - Add support for **transactional data lakes** with versioned table storage.
   - Iceberg allows schema evolution, time-travel queries, and optimized merge/update operations over large-scale S3 or MinIO datasets.

3. **Trino / Presto for Query Federation**  
   - Use **Trino** as a federated query engine to join across:
     - Iceberg (MinIO)
     - MotherDuck / DuckDB
     - External APIs or relational DBs
   - Enables scalable ad hoc SQL over heterogeneous storage systems.

4. **MLFlow + Real-Time Model Deployment**  
   - Incorporate model training and tracking using MLFlow or Vertex AI Pipelines.
   - Enables streaming predictions of battery health, time-to-failure, or anomaly detection directly within Airflow DAGs.

5. **Advanced Stream Processing with Flink**  
   - For near real-time CEP (Complex Event Processing), battery alerts, or predictive maintenance signals.
   - Flink offers event time semantics and windowed joins over Kafka/Redpanda topics.

6. **Distributed Feature Store**  
   - Use Feast or Hopsworks to serve time-aware features to downstream ML models across batch + stream.

7. **Streaming Metadata Catalog**  
   - Integrate with Amundsen or DataHub for lineage tracking, schema monitoring, and team collaboration.
