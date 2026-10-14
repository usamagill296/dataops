"""
DataOps Airflow DAG
===================
Schedules and orchestrates the ETL pipeline

Runs every day at 6am automatically!
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Settings for all tasks
default_args = {
    'owner': 'usama',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'dataops_pipeline',
    default_args=default_args,
    description='DataOps ETL Pipeline',
    schedule_interval='0 6 * * *',  # 6am every day
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=['dataops', 'etl']
)

# Task 1: Check source file exists
def check_source():
    import os
    if not os.path.exists('data/transactions.csv'):
        raise FileNotFoundError("Source file missing!")
    print("✅ Source file found!")

# Task 2: Run ETL pipeline
def run_etl():
    import sys
    sys.path.append('src')
    from pipeline import run_pipeline
    run_pipeline()

# Task 3: Validate output
def validate_output():
    import os, json
    path = 'data/output/valid_transactions.json'
    if not os.path.exists(path):
        raise FileNotFoundError("Output missing!")
    with open(path) as f:
        data = json.load(f)
    if len(data) == 0:
        raise ValueError("No valid records!")
    print(f"✅ Validated {len(data)} records!")

# Create tasks
t1 = PythonOperator(
    task_id='check_source_file',
    python_callable=check_source,
    dag=dag
)

t2 = PythonOperator(
    task_id='run_etl_pipeline',
    python_callable=run_etl,
    dag=dag
)

t3 = PythonOperator(
    task_id='validate_output',
    python_callable=validate_output,
    dag=dag
)

t4 = BashOperator(
    task_id='notify_success',
    bash_command='echo "✅ Pipeline completed successfully!"',
    dag=dag
)

# Task order: check → etl → validate → notify
t1 >> t2 >> t3 >> t4