"""Backfill histórico a MinIO bronze."""
from datetime import datetime, timedelta, timezone
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from include import pipeline
from include.alerts import notify_failure

CONN = "pg_sismowatch"
DAG_ID = "usgs_backfill"


@dag(dag_id=DAG_ID, schedule=None, start_date=datetime(2024, 1, 1), catchup=False, max_active_runs=1,
     tags=["usgs", "bronze", "backfill", "minio"], doc_md=__doc__,
     default_args={"retries": 4, "retry_delay": timedelta(seconds=30), "retry_exponential_backoff": True,
                   "execution_timeout": timedelta(minutes=15), "on_failure_callback": notify_failure})
def usgs_backfill():
    @task()
    def windows(params=None):
        days = int((params or {}).get("days", 30))
        today = datetime.now(timezone.utc).date()
        return [(today - timedelta(days=d)).isoformat() for d in range(days, 0, -1)]

    @task()
    def ingest_day(d: str):
        start = datetime.fromisoformat(d).replace(tzinfo=timezone.utc)
        return pipeline.ingest_window(start, start + timedelta(days=1))

    w = windows()
    ingest_day.expand(d=w)


usgs_backfill()
