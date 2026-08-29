"""
Orchestrates the pipeline: poll the REST API -> upsert into Postgres.
The CDC half (Debezium -> Kafka -> ClickHouse) runs continuously and
independently as a streaming connector, so it isn't a DAG task. Once new rows
have had time to land in ClickHouse via CDC, dbt rebuilds staging -> marts.
"""
import sys
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

from airflow import DAG

sys.path.append("/opt/airflow/project")
from ingestion.ingest_prices import run as ingest_run

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="crypto_prices_pipeline",
    description="REST ingestion -> Postgres -> (Debezium CDC) -> ClickHouse -> dbt",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["inkomoko-assessment"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest_rest_api_to_postgres",
        python_callable=ingest_run,
    )

    # Small buffer so CDC events have time to flow through Kafka into ClickHouse
    # before dbt reads from it. In production this would be a data-freshness
    # sensor against ClickHouse rather than a fixed sleep.
    wait_for_cdc = BashOperator(
        task_id="wait_for_cdc_propagation",
        bash_command="sleep 15",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/project/dbt && /home/airflow/dbt-venv/bin/dbt --log-path /tmp/dbt-logs run --profiles-dir . --target-path /tmp/dbt-target",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/project/dbt && /home/airflow/dbt-venv/bin/dbt --log-path /tmp/dbt-logs test --profiles-dir . --target-path /tmp/dbt-target",
    )

    ingest >> wait_for_cdc >> dbt_run >> dbt_test
