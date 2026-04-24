from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3

def check_aws_costs():
    """Sentinel script to check AWS costs via Boto3."""
    client = boto3.client('ce', region_name='us-east-1')
    response = client.get_cost_and_usage(
        TimePeriod={'Start': '2023-10-01', 'End': '2023-10-02'},
        Granularity='DAILY',
        Metrics=['BlendedCost']
    )
    cost = response['ResultsByTime'][0]['Total']['BlendedCost']['Amount']
    print(f"Pipeline daily run cost: ${cost}")
    # In reality, trigger an alert here if cost > threshold

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ledger_sync_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False
) as dag:

    # 1. Run the Async/DuckDB processing script
    process_telemetry = BashOperator(
        task_id='process_telemetry_duckdb',
        bash_command='python /opt/airflow/processing/usage_processor.py'
    )

    # 2. Run dbt models for SLA reconciliation
    run_dbt = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /opt/airflow/dbt_ledger_sync && dbt run'
    )

    # 3. Sentinel watchdog: Check infrastructure costs
    audit_infrastructure_costs = PythonOperator(
        task_id='audit_infrastructure_costs',
        python_callable=check_aws_costs
    )

    # Define DAG dependencies
    process_telemetry >> run_dbt >> audit_infrastructure_costs