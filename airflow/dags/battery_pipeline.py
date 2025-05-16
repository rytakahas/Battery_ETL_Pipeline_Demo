i# ------------------------------------------
# airflow/dags/battery_pipeline.py
# ------------------------------------------
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
    conn = duckdb.connect("md:battery_db?motherduck_token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6InJ5b2ppLnRha2FoYXNoaUBnbWFpbC5jb20iLCJzZXNzaW9uIjoicnlvamkudGFrYWhhc2hpLmdtYWlsLmNvbSIsInBhdCI6Im5FM3NZLXdENEhWcWR0aUotcmZUazlyZVlkT3VEY21GWWJXaC1QbGVPNWsiLCJ1c2VySWQiOiIwNGZiODAyZS01MjJhLTQ1MDMtOTYyMC1mYmNiNzJjNmJiYjkiLCJpc3MiOiJtZF9wYXQiLCJyZWFkT25seSI6ZmFsc2UsInRva2VuVHlwZSI6InJlYWRfd3JpdGUiLCJpYXQiOjE3NDczMTk4NzV9.dDFrp-nsxROrdV1_QnBNlrAx3E8de0ZUOMfaGLQOiZ4")
    df = conn.execute("""
        SELECT COUNT(*) AS recent_rows
        FROM battery_ts
        WHERE streamed_at >= CURRENT_TIMESTAMP - INTERVAL '1 hour'
    """).fetchdf()
    if df.iloc[0]['recent_rows'] == 0:
        raise ValueError("No new data ingested in the last hour")

def notify():
    print("✅ Pipeline ran successfully")

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
        bash_command='cd /usr/app/dbt_project && dbt run'
    )

    test_dbt = BashOperator(
        task_id='run_dbt_tests',
        bash_command='cd /usr/app/dbt_project && dbt test'
    )

    finish = PythonOperator(
        task_id='notify_success',
        python_callable=notify
    )

    check_data >> run_dbt >> test_dbt >> finish
