from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from kafka import KafkaConsumer
import json

def consume_kafka_messages():
    consumer = KafkaConsumer(
        'battery-data',
        bootstrap_servers='kafka:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='battery-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    for message in consumer:
        data = message.value
        # Process and store data as needed
        print(f"Received data: {data}")

with DAG('battery_pipeline', start_date=datetime(2025, 1, 1), schedule_interval='@hourly') as dag:
    consume_task = PythonOperator(
        task_id='consume_kafka_messages',
        python_callable=consume_kafka_messages
    )

