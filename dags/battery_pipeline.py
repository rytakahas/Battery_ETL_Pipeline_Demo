from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import duckdb
import pandas as pd
import os

# Default DAG arguments
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Connect to MotherDuck
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6InJ5b2ppLnRha2FoYXNoaUBnbWFpbC5jb20iLCJzZXNzaW9uIjoicnlvamkudGFrYWhhc2hpLmdtYWlsLmNvbSIsInBhdCI6Im5FM3NZLXdENEhWcWR0aUotcmZUazlyZVlkT3VEY21GWWJXaC1QbGVPNWsiLCJ1c2VySWQiOiIwNGZiODAyZS01MjJhLTQ1MDMtOTYyMC1mYmNiNzJjNmJiYjkiLCJpc3MiOiJtZF9wYXQiLCJyZWFkT25seSI6ZmFsc2UsInRva2VuVHlwZSI6InJlYWRfd3JpdGUiLCJpYXQiOjE3NDczMTk4NzV9.dDFrp-nsxROrdV1_QnBNlrAx3E8de0ZUOMfaGLQOiZ4")
DB_PATH = f"md:battery_db?motherduck_token={MOTHERDUCK_TOKEN}"

# Check if streaming data has been ingested recently
def check_streamed_data():
    conn = duckdb.connect(DB_PATH)
    tables = conn.execute("SHOW TABLES").fetchdf()
    if "battery_ts_cleaned" not in tables['name'].values:
        print("⚠️ Table 'battery_ts_cleaned' does not exist. Skipping check.")
        return False

    df = conn.execute("""
        SELECT
            COUNT(*) AS total_rows,
            MAX(TRY_CAST(streamed_at AS TIMESTAMP)) AS last_streamed,
            SUM(CASE WHEN TRY_CAST(streamed_at AS TIMESTAMP) >= NOW() - INTERVAL '1 hour' THEN 1 ELSE 0 END) AS last_hour_count
        FROM battery_ts_cleaned
    """).fetchdf()

    print(f"🟡 Total rows: {df.iloc[0]['total_rows']}")
    print(f"⏱️ Last streamed: {df.iloc[0]['last_streamed']}")
    print(f"⚡ Rows in last hour: {df.iloc[0]['last_hour_count']}")
    return True

# Clean and normalize battery data
def clean_data():
    conn = duckdb.connect(DB_PATH)
    df_raw = conn.execute("SELECT * FROM battery_ts_cleaned").fetchdf()

    # Drop all-NA rows and normalize column names
    df_clean = df_raw.dropna(how="all")
    df_clean.columns = [col.strip().lower().replace(" ", "_") for col in df_clean.columns]

    # Attempt numeric conversion
    for col in df_clean.columns:
        try:
            df_clean[col] = pd.to_numeric(df_clean[col])
        except Exception:
            continue

    # Optional: filter by capacity outliers
    if "capacity" in df_clean.columns:
        q_low = df_clean["capacity"].quantile(0.01)
        q_high = df_clean["capacity"].quantile(0.99)
        df_clean = df_clean[(df_clean["capacity"] >= q_low) & (df_clean["capacity"] <= q_high)]

    # Save intermediate cleaned table
    conn.execute("DROP TABLE IF EXISTS battery_cleaned")
    conn.register("cleaned_view", df_clean)
    conn.execute("CREATE TABLE battery_cleaned AS SELECT * FROM cleaned_view")
    print("🧼 Cleaned data stored in 'battery_cleaned'")

    # Save final warehouse table for analysis
    conn.execute("DROP TABLE IF EXISTS battery_ts")
    conn.execute("CREATE TABLE battery_ts AS SELECT * FROM battery_cleaned")
    print("📄 Final table 'battery_ts' refreshed from 'battery_cleaned'")

# Notify end of pipeline
def notify():
    print("✅ Pipeline ran successfully")

# Define DAG
with DAG(
    dag_id='battery_pipeline',
    default_args=DEFAULT_ARGS,
    schedule='@hourly',
    catchup=False
) as dag:

    check_data = PythonOperator(
        task_id='check_streamed_data',
        python_callable=check_streamed_data
    )

    clean_table = PythonOperator(
        task_id='clean_battery_data',
        python_callable=clean_data
    )

    run_dbt = BashOperator(
        task_id='run_dbt_cleaning',
        bash_command='cd /usr/local/airflow/dbt_project && dbt run --profiles-dir /home/astro/.dbt'
    )

    test_dbt = BashOperator(
        task_id='run_dbt_tests',
        bash_command='cd /usr/local/airflow/dbt_project && dbt test --profiles-dir /home/astro/.dbt'
    )

    finish = PythonOperator(
        task_id='notify_success',
        python_callable=notify
    )

    check_data >> clean_table >> run_dbt >> test_dbt >> finish

