from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineering_team',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('ledger_sync_kafka', default_args=default_args, schedule='@daily', catchup=False) as dag:

    
    ingest_from_kafka = BashOperator(
        task_id='consume_kafka_to_bronze',
        bash_command='python /usr/local/airflow/include/processing/kafka_consumer_to_s3.py'
    )

    data_quality_check = BashOperator(
        task_id='great_expectations_gate',
        bash_command='python /usr/local/airflow/include/processing/data_quality_gate.py'
    )

    process_silver = BashOperator(
        task_id='duckdb_process_silver',
        bash_command='python /usr/local/airflow/include/processing/usage_processor.py'
    )

    run_dbt_gold = BashOperator(
        task_id='dbt_transform_gold',
        bash_command='cd /usr/local/airflow/include/dbt_ledger_sync && dbt run --profiles-dir .'
    )

    ingest_from_kafka >> data_quality_check >> process_silver >> run_dbt_gold