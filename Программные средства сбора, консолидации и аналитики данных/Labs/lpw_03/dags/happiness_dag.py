from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from happiness_etl import extract, load_raw, load_staging, create_datamart, validate_datamart

with DAG(
    dag_id='world_happiness_variant_09',
    description='Kaggle -> RAW -> STG -> SQL VIEW. World Happiness 2015-2019.',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    default_args={'owner': 'ivan', 'retries': 2, 'retry_delay': timedelta(minutes=1)},
    tags=['lab03', 'variant09', 'happiness'],
) as dag:
    download = PythonOperator(task_id='extract_kaggle', python_callable=extract)
    raw = PythonOperator(task_id='load_raw', python_callable=load_raw)
    staging = PythonOperator(task_id='transform_and_load_stg', python_callable=load_staging,
                             op_kwargs={'run_id': '{{ run_id }}'})
    mart = PythonOperator(task_id='create_sql_view', python_callable=create_datamart)
    validate = PythonOperator(task_id='validate_datamart', python_callable=validate_datamart,
                              op_kwargs={'run_id': '{{ run_id }}'})
    download >> raw >> staging >> mart >> validate
