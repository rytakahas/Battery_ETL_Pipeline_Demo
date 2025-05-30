from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id="battery_etl_bigquery",
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["battery", "bigquery", "spark"],
) as dag:

    spark_clean_bq = SparkSubmitOperator(
        task_id="clean_battery_data",
        application="/usr/local/airflow/include/battery_clean_bq.py",
        conn_id="spark_default",  # Ensure this connection exists in Airflow
        conf={
            "spark.master": "local[*]",
        },
        dag=dag
    )

    spark_clean_bq

