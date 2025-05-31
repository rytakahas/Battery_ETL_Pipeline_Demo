
# Battery Degradation Analytics Pipeline (BigQuery Version)

This project builds a **modern streaming data pipeline** to ingest, store, clean, and transform **battery degradation data** using:

- **Google Pub/Sub** for real-time ingestion
- **Google Cloud Storage (GCS)** as a raw data lake
- **BigQuery** as the data warehouse
- **dbt** for SQL-based data transformations
- **Airflow (via Astro)** for orchestration

---

## Architecture Overview

```plaintext
[Google Pub/Sub]  →  [Python Producer (Streaming)]  
     ↓
[GCS Raw Bucket (battery_raw/)]
     ↓
[Airflow DAG (Astro)] → Triggers:
    • GCS to BigQuery ingestion
    • Data cleaning and transformation
    • dbt model refreshes and validation
    • Logging and monitoring
     ↓
[BigQuery Staging → battery_ts_cleaned]
     ↓
[dbt Models] → battery_ts (view), battery_cleaned (table), battery_features (table)
     ↓
[BigQuery Analytics Dataset]
```

---

## Project Structure

```bash
├── Dockerfile
├── README.md
├── airflow/
│   └── dags/
│       └── battery_pipeline.py
├── dags/
│   ├── battery_pipeline.py
│   └── exampledag.py
├── docker-compose.yml
├── include/
│   └── .dbt/
│       └── profiles.yml
└── notebook/
    └── ingest_battery_stream.ipynb

.env                            # Stores GCP credentials and tokens                     
```

---

## Setup Instructions

### 1. Clone Repository

```bash
git clone https://github.com/your-org/battery-pipeline
cd battery-pipeline
```

### 2. Create `.env` File

```env
GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/creds.json
GCP_PROJECT_ID=your-gcp-project
BQ_DATASET=battery_data
GCS_BUCKET=battery-raw-data
PUBSUB_TOPIC=battery-stream
```

### 3. Start Astro Dev Environment

```bash
astro dev start
```

### 4. Run Pub/Sub Producer

```python
# Stream producer to Pub/Sub
from google.cloud import pubsub_v1
# Publishing battery data into 'battery-stream' topic
```

---

## dbt Models

| Model               | Type   | Description                                                  |
|--------------------|--------|--------------------------------------------------------------|
| battery_ts.sql     | view   | Thin view over raw data for easy reference                   |
| battery_cleaned.sql| table  | Removes nulls, enforces types, and performs QA               |
| battery_features.sql| table | Adds power_watt, total_capacity, time delta between readings |

---

## Airflow DAG

**Location**: `airflow/dags/battery_pipeline.py`

Runs every hour and:

- Ingests new data from GCS → BigQuery
- Runs dbt `run` and `test`
- Logs pipeline status and alerts on errors

---

## Query Examples in BigQuery

```sql
-- Count rows by source file
SELECT source_file, COUNT(*) 
FROM `battery_data.battery_ts` 
GROUP BY source_file;

-- Compute max voltage by timestamp
SELECT MAX(CAST(voltage_v AS FLOAT64)), streamed_at 
FROM `battery_data.battery_ts` 
GROUP BY streamed_at 
ORDER BY streamed_at DESC;

-- Compute power and capacity
SELECT
  *,
  CAST(voltage_v AS FLOAT64) * CAST(current_a AS FLOAT64) AS power_watt,
  CAST(charge_capacity_ah AS FLOAT64) + CAST(discharge_capacity_ah AS FLOAT64) AS total_capacity
FROM `battery_data.battery_cleaned`;
```

---

## Requirements

- Python 3.9+
- Docker
- Astro CLI
- GCP Project + IAM access
- dbt-bigquery + Google SDK

---

## Future Enhancements

- Integrate with **Apache Beam** or **Dataflow** for serverless ETL
- Add **Vertex AI** for streaming battery health predictions
- Use **Looker** or **Dashboards** for live metrics
- Implement **Data Catalog** + **BigLake** + **Lineage Tracking**

---

## Acknowledgements

- CALCE Battery Dataset
- Google BigQuery and Cloud Pub/Sub
- Astronomer (Airflow)
- dbt Labs
