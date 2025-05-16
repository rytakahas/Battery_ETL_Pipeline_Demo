from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import duckdb

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def check_streamed_data():
    conn = duckdb.connect("md:battery_db?motherduck_token=YOUR_TOKEN")
    df = conn.execute("""
        SELECT COUNT(*) AS recent_rows
        FROM battery_ts
        WHERE streamed_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
    """).fetchdf()
    if df.iloc[0]['recent_rows'] == 0:
        raise ValueError("No new data ingested in the last hour")

def notify():
    print("Pipeline ran successfully")

with DAG(
    dag_id='battery_pipeline',
    default_args=DEFAULT_ARGS,
    schedule_interval='@hourly',
    catchup=False
) as dag:

    check_data = PythonOperator(
        task_id='check_streamed_data',
        python_callable=check_streamed_data
    )

    run_dbt = BashOperator(
        task_id='run_dbt_cleaning',
        bash_command='cd /usr/local/airflow/dbt_project && dbt run'
    )

    test_dbt = BashOperator(
        task_id='run_dbt_tests',
        bash_command='cd /usr/local/airflow/dbt_project && dbt test'
    )

    finish = PythonOperator(
        task_id='notify_success',
        python_callable=notify
    )

    # DAG task dependency chain
    check_data >> run_dbt >> test_dbt >> finish

