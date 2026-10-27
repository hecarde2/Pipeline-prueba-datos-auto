"""Transforma bronze (MinIO) -> silver -> gold con validación de calidad."""
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.datasets import Dataset
from include import pipeline
from include.alerts import notify_failure

CONN = "pg_sismowatch"
DAG_ID = "usgs_transform_quality"
BRONZE = Dataset("s3://bronze/raw")


@dag(dag_id=DAG_ID, schedule=[BRONZE], start_date=datetime(2024, 1, 1), catchup=False, max_active_runs=1,
     tags=["usgs", "silver", "gold", "calidad", "minio"], doc_md=__doc__,
     default_args={"retries": 2, "retry_delay": timedelta(seconds=30), "retry_exponential_backoff": True,
                   "execution_timeout": timedelta(minutes=10), "on_failure_callback": notify_failure})
def usgs_transform_quality():
    @task()
    def to_silver():
        return pipeline.load_silver()

    @task()
    def to_gold(summary: dict):
        return pipeline.publish_gold()

    @task()
    def checks(summary: dict):
        return pipeline.final_checks(summary)

    s = to_silver()
    g = to_gold(s)
    checks(s)


usgs_transform_quality()
