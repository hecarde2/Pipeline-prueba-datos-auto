"""Descarga el feed `all_hour` de USGS y lo guarda en MinIO bronze."""
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from include import pipeline
from include.alerts import notify_failure

CONN = "pg_sismowatch"
DAG_ID = "usgs_ingest"


@dag(dag_id=DAG_ID, schedule="*/5 * * * *", start_date=datetime(2024, 1, 1), catchup=False,
     max_active_runs=1, tags=["usgs", "bronze", "ingesta"], doc_md=__doc__,
     default_args={"retries": 5, "retry_delay": timedelta(seconds=20), "retry_exponential_backoff": True,
                   "max_retry_delay": timedelta(minutes=3), "execution_timeout": timedelta(minutes=4),
                   "on_failure_callback": notify_failure})
def usgs_ingest():
    @task()
    def fetch_to_bronze() -> dict:
        return pipeline.ingest_feed()

    @task()
    def verify(info: dict):
        return pipeline.verify_bronze(info["bronze_id"])

    verify(fetch_to_bronze())


usgs_ingest()
