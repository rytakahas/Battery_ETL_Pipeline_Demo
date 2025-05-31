
# Battery Degradation Analytics Pipeline (BigQuery + Kafka Version)

This project builds a **modern streaming data pipeline** to ingest, store, clean, and transform **battery degradation data** using:

- **Google Pub/Sub** or **Apache Kafka** for real-time ingestion
- **Google Cloud Storage (GCS)** as a raw data lake
- **BigQuery** as the data warehouse
- **dbt** for SQL-based data transformations
- **Airflow (via Astro)** for orchestration

---

## Architecture Overview

```plaintext
Option A: Google Cloud Pub/Sub
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

Option B: Kafka (Redpanda)
[Kafka Topic (battery-data)]  
     ↓
[Python Kafka Consumer → Write to GCS]
     ↓
[GCS Raw Bucket (battery_raw/)]
     ↓
[Same downstream pipeline as Pub/Sub]
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

---

## Streaming via Kafka (Alternative to Pub/Sub)

Install:

```bash
pip install kafka-python
```

**Kafka Producer Example**:

```python
from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

data = {
    "voltage_v": 4.2,
    "current_a": 1.5,
    "streamed_at": "2024-06-01T12:00:00Z",
}

producer.send('battery-data', value=data)
producer.flush()
```

**Kafka Consumer Writing to GCS**:

```python
from kafka import KafkaConsumer
from google.cloud import storage
import json

consumer = KafkaConsumer(
    'battery-data',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

client = storage.Client()
bucket = client.get_bucket('battery-raw-data')

for msg in consumer:
    blob = bucket.blob(f'battery_raw/stream_{msg.timestamp}.json')
    blob.upload_from_string(json.dumps(msg.value), content_type='application/json')
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
- Kafka (Optional)

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
- Apache Kafka / Redpanda
- Astronomer (Airflow)
- dbt Labs
