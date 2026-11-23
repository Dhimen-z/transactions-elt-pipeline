"""DAG Airflow : orchestration quotidienne du pipeline.

ingest (bronze) -> dbt build (staging + marts + tests) -> contrôles qualité.
Le DAG suppose que le dépôt est monté dans /opt/pipeline (image du Dockerfile ou volume).
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/pipeline"

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="transactions_elt",
    start_date=datetime(2026, 1, 1),
    schedule="0 3 * * *",
    catchup=False,
    default_args=default_args,
    tags=["elt", "dbt", "duckdb"],
) as dag:
    ingest = BashOperator(task_id="ingest_bronze", bash_command=f"cd {PROJECT} && python -m pipeline.run ingest")
    dbt_build = BashOperator(task_id="dbt_build", bash_command=f"cd {PROJECT} && python -m pipeline.run transform")
    checks = BashOperator(task_id="quality_checks", bash_command=f"cd {PROJECT} && python -m pipeline.run quality")

    ingest >> dbt_build >> checks
