from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import duckdb
import pandas as pd
import os
import boto3

# Default DAG arguments
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Connect to MotherDuck
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
DB_PATH = f"md:battery_db?motherduck_token={MOTHERDUCK_TOKEN}"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "https://m3g2.ldn.idrivee2-66.com")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "qy1bbnyZNrTbkzd63k7d")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadminD7yiFWqeYUYGykqrEvtVJa6il4bWKVtfwnN0Wop3")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "battery-data")

# Check if streaming data has been ingested recently
def check_streamed_data():
    s3 = boto3.client('s3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    objects = s3.list_objects_v2(Bucket=MINIO_BUCKET)
    if 'Contents' not in objects:
        print("No files found in MinIO.")
        return False
    print(f"Found {len(objects['Contents'])} files in MinIO bucket.")
    return True

# Clean and normalize battery data from MinIO and store in MotherDuck
def clean_data():
    s3 = boto3.client('s3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    response = s3.list_objects_v2(Bucket=MINIO_BUCKET)
    latest_file = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)[0]['Key']
    obj = s3.get_object(Bucket=MINIO_BUCKET, Key=latest_file)
    df_raw = pd.read_csv(obj['Body'])

    df_clean = df_raw.dropna(how="all")
    df_clean.columns = [col.strip().lower().replace(" ", "_") for col in df_clean.columns]
    for col in df_clean.columns:
        try:
            df_clean[col] = pd.to_numeric(df_clean[col])
        except Exception:
            continue
    if "capacity" in df_clean.columns:
        q_low = df_clean["capacity"].quantile(0.01)
        q_high = df_clean["capacity"].quantile(0.99)
        df_clean = df_clean[(df_clean["capacity"] >= q_low) & (df_clean["capacity"] <= q_high)]

    conn = duckdb.connect(DB_PATH)
    conn.execute("DROP TABLE IF EXISTS battery_cleaned")
    conn.register("cleaned_view", df_clean)
    conn.execute("CREATE TABLE battery_cleaned AS SELECT * FROM cleaned_view")
    print("Cleaned data stored in MotherDuck")

def notify():
    print("Pipeline ran successfully")

with DAG(
    dag_id='battery_pipeline_minio_motherduck',
    default_args=DEFAULT_ARGS,
    schedule='@hourly',
    catchup=False
) as dag:

    check_data = PythonOperator(
        task_id='check_minio_raw_data',
        python_callable=check_streamed_data
    )

    clean_table = PythonOperator(
        task_id='clean_and_store_to_motherduck',
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

