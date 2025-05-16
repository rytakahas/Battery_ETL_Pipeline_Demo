## Battery Analytics Streaming Pipeline (Redpanda (Kafka) + MotherDuck (DB) + dbt + Astro)

This project builds a modern data pipeline to ingest battery degradation data using Kafka (Redpanda), store it in MotherDuck, clean and transform it using dbt, and monitor everything using Airflow via Astro.

---

### Architecture Overview

```plaintext
[Redpanda Kafka]  →  [Python Producer (Streaming)]  
     ↓
[Raw Table in MotherDuck (battery_ts)]
     ↓
[dbt Models] → [battery_cleaned, battery_features]
     ↓
[Airflow DAG (Astro)] → Orchestrates:
    • Kafka streaming checks
    • dbt model refreshes
    • Alerts + logging

```



### Pipeline Structure
```bash
/airflow/
  dags/
    battery_pipeline.py         # Main DAG
/dbt_project/
  models/
    battery_ts.sql              # Raw
    battery_cleaned.sql         # Cleaned
    battery_features.sql        # Engineered
  dbt_project.yml
```
