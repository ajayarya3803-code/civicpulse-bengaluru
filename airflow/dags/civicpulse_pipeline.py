from datetime import datetime

from airflow.sdk import dag
from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator,
)


@dag(
    dag_id="civicpulse_pipeline",
    description="Orchestrate the CivicPulse Bengaluru data pipeline",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["civicpulse", "databricks", "data-engineering"],
)
def civicpulse_pipeline():
    DatabricksRunNowOperator(
        task_id="run_civicpulse_databricks_job",
        databricks_conn_id="databricks_default",
        job_id=660144243332978,
        wait_for_termination=True,
    )


civicpulse_pipeline()
