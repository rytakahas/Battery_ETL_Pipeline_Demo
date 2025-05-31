# battery_data_ingest.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import requests
import zipfile
import pandas as pd
import json
from urllib.parse import urlparse, unquote
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from confluent_kafka import Consumer

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def extract_and_upload_to_bq():
    project_id = "able-balm-454718-n8"
    dataset_id = "battery_sandbox"
    table_prefix = "battery_data"
    client = bigquery.Client(project=project_id)

    try:
        client.get_dataset(dataset_id)
    except NotFound:
        dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
        client.create_dataset(dataset)

    dataset_urls = [
        "https://web.calce.umd.edu/batteries/data/SP1_Initial%20capacity_10_16_2015.zip",
        "https://web.calce.umd.edu/batteries/data/SP2_25C_FUDS.zip"
    ]

    for zip_url in dataset_urls:
        parsed_url = urlparse(zip_url)
        zip_filename = os.path.basename(unquote(parsed_url.path))
        zip_path = os.path.join("/tmp/battery_data", zip_filename)
        extract_dir = os.path.join("/tmp/battery_data", os.path.splitext(zip_filename)[0])

        os.makedirs("/tmp/battery_data", exist_ok=True)

        if not os.path.exists(zip_path):
            r = requests.get(zip_url, verify=False)
            with open(zip_path, "wb") as f:
                f.write(r.content)

        if not os.path.exists(extract_dir):
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(extract_dir)

        files = os.listdir(extract_dir)
        target_file = next((os.path.join(extract_dir, f) for f in files if f.lower().endswith((".xlsx", ".xls"))), None)
        if not target_file:
            raise FileNotFoundError(f"No Excel file in {extract_dir}.")

        df = pd.read_excel(target_file, engine="openpyxl")
        df = df.dropna(how='all').reset_index(drop=True)
        df.columns = [str(col).strip().lower().replace(" ", "_").replace("(", "").replace(")", "")
                      .replace("/", "_").replace("%", "percent") for col in df.columns]
        df = df.loc[:, ~df.columns.str.contains('^unnamed', case=False)]

        df["id"] = df.index
        df["source_file"] = zip_filename
        df["loaded_at"] = pd.Timestamp.utcnow()

        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].astype(str)

        for col in df.columns:
            if df[col].dtype == "object":
                try:
                    if df[col].str.match(r"^\d{4}-\d{2}-\d{2}").any():
                        df[col] = pd.to_datetime(df[col], errors="coerce")
                except Exception:
                    pass

        clean_table_name = os.path.splitext(zip_filename)[0].replace('-', '_').replace(' ', '_').lower()
        table_id = f"{project_id}.{dataset_id}.{table_prefix}_{clean_table_name}"

        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition="WRITE_TRUNCATE"
        )
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

def consume_from_kafka_and_upload():
    project_id = "able-balm-454718-n8"
    dataset_id = "battery_sandbox"
    table_id = f"{project_id}.{dataset_id}.battery_kafka_stream"

    conf = {
        'bootstrap.servers': 'kafka:9092',  # or localhost:9092 if local
        'group.id': 'battery-consumer-group',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(conf)
    consumer.subscribe(['battery-stream'])

    client = bigquery.Client(project=project_id)
    messages = []

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                break
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue

            record = json.loads(msg.value().decode('utf-8'))
            messages.append(record)

        if messages:
            df = pd.DataFrame(messages)
            df['streamed_at'] = pd.Timestamp.utcnow()
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                autodetect=True
            )
            client.load_table_from_dataframe(df, table_id, job_config=job_config).result()

    finally:
        consumer.close()

with DAG(
    'battery_data_ingest',
    default_args=default_args,
    description='Download + Kafka ingest battery data to BigQuery',
    schedule_interval='@hourly',
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    batch_ingest_task = PythonOperator(
        task_id='extract_and_upload_to_bq',
        python_callable=extract_and_upload_to_bq
    )

    kafka_stream_task = PythonOperator(
        task_id='consume_from_kafka_and_upload',
        python_callable=consume_from_kafka_and_upload
    )

    batch_ingest_task >> kafka_stream_task

