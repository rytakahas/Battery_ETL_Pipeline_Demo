from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id="battery_etl_bigquery",
    default_args=default_args,
    description="Spark job to clean battery data in BigQuery",
    schedule_interval=None,  # Change to "@daily" or custom cron if needed
    start_date=days_ago(1),
    catchup=False,
    tags=["battery", "bigquery", "spark"],
) as dag:

    # Task: Clean raw battery data
    spark_clean_bq = SparkSubmitOperator(
        task_id="clean_battery_data",
        application="/usr/local/airflow/include/battery_clean_bq.py",
        conn_id="spark_default",  # Make sure this connection is defined in Airflow UI
        conf={
            "spark.master": "local[*]",
            "spark.executor.memory": "2g",
        },
        application_args=[
            "--input", "battery_sandbox.battery_ts_raw",
            "--output", "battery_sandbox.battery_ts_cleaned"
        ]
    )

    # If you have feature generation, you can chain them
    # spark_features_bq = SparkSubmitOperator(
    #     task_id="generate_battery_features",
    #     application="/usr/local/airflow/include/battery_features_bq.py",
    #     conn_id="spark_default",
    #     conf={"spark.master": "local[*]"},
    # )

    # spark_clean_bq >> spark_features_bq

    spark_clean_bq

